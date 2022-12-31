import os
import shutil
import pathlib
import hashlib
import pickle
import json
# import protobuf
from enum import Enum
import tempfile
from typing import *

# BLOCK_SIZE is the size of the blocks used to compute the block library
BLOCK_SIZE = 8

# OperationType is an enumeration type that defines the different types of operations that can be performed when applying a patch. It could be defined as follows:
class OperationType(Enum):
    DATA = 1
    BLOCK_RANGE = 2
    SYMLINK = 3
    DELETE = 4
    MKDIR = 5

class Operation:
    def __init__(self, op_type: OperationType, file_index: int = 0, block_index: int = 0, block_span: int = 0, data: bytes = b'', target_path: str = ''):
        self.op_type = op_type
        self.file_index = file_index
        self.block_index = block_index
        self.block_span = block_span
        self.data = data
        self.target_path = target_path
    
class Patch:
    def __init__(self, old_dirs, new_dirs, deleted_files, extra_files, operations):
        self.old_dirs = old_dirs
        self.new_dirs = new_dirs
        self.deleted_files = deleted_files
        self.extra_files = extra_files
        self.operations = operations




## Error classes
class PatchApplyError(Exception):
    def __init__(self, message, file=None, operation=None):
        super().__init__(message)
        self.file = file
        self.operation = operation


import os
from tempfile import TemporaryDirectory
from typing import List
### APPLY PATCH ###
def create_missing_dirs(patch: Patch, staging_dir: str):
  """Create any missing directories in the staging directory."""
  for dir_path in patch.new_dirs:
    os.makedirs(os.path.join(staging_dir, dir_path), exist_ok=True)

def apply_operations(patch: Patch, old_dir: str, staging_dir: str):
  """Apply the operations in the patch to the old directory and write the results to the staging directory."""
  for operation in patch.operations:
    print(f'Applying operation to {operation.target_path}', old_dir, staging_dir)
    if operation.op_type == OperationType.DATA:
      # Write the data to the new file
      new_file_path = os.path.join(staging_dir, operation.target_path)
      with open(new_file_path, 'wb') as f:
        f.write(operation.data)
    elif operation.op_type == OperationType.BLOCK_RANGE:
      # Open the old file and seek to the correct offset
      old_file_path = os.path.join(old_dir, operation.target_path)
      with open(old_file_path, 'rb') as old_file:
        old_file.seek(operation.block_index * BLOCK_SIZE)
        # Read the specified number of blocks from the old file
        data = old_file.read(operation.block_span * BLOCK_SIZE)
      # Write the block range to the new file
      new_file_path = os.path.join(staging_dir, operation.target_path)
      with open(new_file_path, 'wb') as f:
        f.write(data)


def delete_extra_files(patch: Patch, new_dir: str):
  """Delete any files or symlinks in the new directory but not in the old directory."""
  for file in patch.extra_files:
    file_path = os.path.join(new_dir, file)
    if os.path.isfile(file_path) or os.path.islink(file_path):
      os.remove(file_path)
    elif os.path.isdir(file_path):
      os.rmdir(file_path)

## Rolling hash
#  start with a fixed-size window that slides over the stream of data, one chunk at a time. 
#  As the window moves, you update the hash value by taking into account the new chunk of data that has been added to the window and the old chunk of data that has been removed from the window.    
class RollingHash:
  def __init__(self, window_size=1024, prime=1000000007):
    self.window_size = window_size
    self.prime = prime
    self.hash_value = 0
    self.window = b''

  def update(self, new_chunk, old_chunk):
    self.window = self.window[len(old_chunk):] + new_chunk
    self.hash_value = (self.hash_value * self.prime + new_chunk) % self.prime
    self.hash_value = (self.hash_value - old_chunk * pow(self.prime, self.window_size, self.prime)) % self.prime

  def strong_hash(self):
    # Return a strong hash value based on the current window
    return self.hash_value


## DIFF
def diff_files(old_file_path: str, new_file_path: str) -> List[Operation]:
    operations = []

    # Open the old and new files
    with open(old_file_path, 'rb') as old_file, open(new_file_path, 'rb') as new_file:
        old_chunk = old_file.read(BLOCK_SIZE)
        new_chunk = new_file.read(BLOCK_SIZE)
        block_index = 0
        # Compare the files chunk by chunk
        while old_chunk or new_chunk:
            if old_chunk == new_chunk:
                # The chunks are the same, move to the next chunk
                block_index += 1
                old_chunk = old_file.read(BLOCK_SIZE)
                new_chunk = new_file.read(BLOCK_SIZE)
            else:
                # The chunks are different, find the next matching chunk
                matching_block_index = find_next_matching_block(old_file, new_file, old_chunk, new_chunk)
                if matching_block_index is not None:
                    # A matching chunk was found, create a BLOCK_RANGE operation for the blocks in between
                    block_span = matching_block_index - block_index
                    operations.append(Operation(OperationType.BLOCK_RANGE, block_index=block_index, block_span=block_span))
                    block_index = matching_block_index
                    old_file.seek(block_index * BLOCK_SIZE)
                    new_file.seek(block_index * BLOCK_SIZE)
                    old_chunk = old_file.read(BLOCK_SIZE)
                    new_chunk = new_file.read(BLOCK_SIZE)
                else:
                    # No matching chunk was found, create a DATA operation for the rest of the new file
                    operations.append(Operation(OperationType.DATA, data=new_file.read()))
                    break
    return operations

def diff(old_dir: str, new_dir: str) -> Patch:
    # Scan the old and new directories
    old_dirs, old_files = scan_dir(old_dir)
    new_dirs, new_files = scan_dir(new_dir)

    # Find the deleted and extra files
    deleted_files = set(old_files) - set(new_files)
    extra_files = set(new_files) - set(old_files)

    # Initialize the list of operations
    operations = []

    # Process the deleted files
    for file in deleted_files:
        operations.append(Operation(OperationType.DELETE, target_path=file))

    # Process the extra files
    for file in extra_files:
        file_path = os.path.join(new_dir, file)
        if os.path.islink(file_path):
            # File is a symlink, add a SYMLINK operation
            with open(file_path, 'r') as f:
                target_path = f.read()
            operations.append(Operation(OperationType.SYMLINK, target_path=target_path))
        elif os.path.isdir(file_path):
            # File is a directory, add a MKDIR operation

            operations.append(Operation(OperationType.MKDIR, target_path=file))
        else:
            # File is a regular file, add a DATA operation
            with open(file_path, 'rb') as f:
                data = f.read()
            operations.append(Operation(OperationType.DATA, data=data, target_path=file))

    # Process the common files
    common_files = set(old_files) & set(new_files)
    for file in common_files:
        old_file_path = os.path.join(old_dir, file)
        new_file_path = os.path.join(new_dir, file)
        if os.path.islink(old_file_path) or os.path.islink(new_file_path):
            # File is a symlink, add a SYMLINK operation
            with open(new_file_path, 'r') as f:
                target_path = f.read()
            operations.append(Operation(OperationType.SYMLINK, target_path=target_path))
        elif os.path.isdir(old_file_path) or os.path.isdir(new_file_path):
            # File is a directory, add a MKDIR operation
            operations.append(Operation(OperationType.MKDIR, target_path=file))
        else:
            # File is a regular file, compare the contents
            with open(old_file_path, 'rb') as old_file:
                with open(new_file_path, 'rb') as new_file:
                    old_chunk = old_file.read(BLOCK_SIZE)

                    new_chunk = new_file.read(BLOCK_SIZE)
                    # print number of chunnks in both files
                    

                    while old_chunk and new_chunk:
                        if old_chunk != new_chunk:
                            # Find the next matching block in the new file
                            matching_block = find_next_matching_block(old_chunk, new_file)
                            if matching_block:
                                # Add a BLOCK_RANGE operation
                                block_index = new_file.tell() // BLOCK_SIZE - 1
                                block_span = len(matching_block) // BLOCK_SIZE
                                operations.append(Operation(OperationType.BLOCK_RANGE, block_index=block_index, block_span=block_span, target_path=file))
                                new_chunk = matching_block + new_file.read(BLOCK_SIZE)
                            else:
                                # Add a DATA operation
                                operations.append(Operation(OperationType.DATA, data=new_chunk, target_path=file))
                                break
                        old_chunk = old_file.read(BLOCK_SIZE)
                        new_chunk = new_file.read(BLOCK_SIZE)
                    else:
                        if not old_chunk and new_chunk:
                            # Add a DATA operation for the remaining data in the new file
                            operations.append(Operation(OperationType.DATA, data=new_chunk, target_path=file))
    # Return the patch
    return Patch(old_dirs=old_dirs, new_dirs=new_dirs, deleted_files=deleted_files, extra_files=extra_files, operations=operations)


def scan_dir(dir_path: str) -> tuple[List[str], List[str]]:
    dirs = []
    files = []
    for entry in os.scandir(dir_path):
        if entry.is_dir():
            dirs.append(entry.name)
        elif entry.is_file():
            files.append(entry.name)
    return dirs, files

# def apply_patch(patch: Patch, old_dir: str, staging_dir: str) -> None:
#     # Create any missing directories in the staging directory
#     create_missing_dirs(patch, staging_dir)

#     # Apply the operations in the patch to the old directory and write the results to the staging directory
#     apply_operations(patch, old_dir, staging_dir)

#     # Delete any extra files or symlinks in the new directory
#     delete_extra_files(patch, staging_dir)


def apply_patch(patch, old_dir, new_dir):
    # Create the staging directory
    staging_dir = tempfile.mkdtemp()

    print(f'Apply patch staging_dir: {staging_dir}')
    print(f'Apply patch new_dir: {new_dir}')
    print(f'Apply patch old_dir: {old_dir}')
    # Create any missing directories in the staging directory
    create_missing_dirs(patch, staging_dir)
    # Apply the operations in the patch to the old directory and write the results to the staging directory
    for operation in patch.operations:
        print(f'Applying operation to {operation.target_path}', operation.op_type, operation.data, old_dir, new_dir, staging_dir)
        if operation.op_type == OperationType.DATA:
            # Write the data to the new file
            new_file_path = os.path.join(staging_dir, operation.target_path)
            with open(new_file_path, 'wb') as f:
                f.write(operation.data)
        elif operation.op_type == OperationType.BLOCK_RANGE:
            # Open the old file and seek to the correct offset
            old_file_path = os.path.join(old_dir, operation.target_path)
            with open(old_file_path, 'rb') as old_file:
                old_file.seek(operation.block_index * BLOCK_SIZE)
                # Read the specified number of blocks from the old file
                data = old_file.read(operation.block_span * BLOCK_SIZE)
            # Write the block range to the new file
            new_file_path = os.path.join(staging_dir, operation.target_path)
            with open(new_file_path, 'wb') as f:
                f.write(data)
    # Delete any extra files in the new directory
    delete_extra_files(patch, new_dir)
    # Replace the old directory with the new directory
    shutil.rmtree(new_dir)
    shutil.move(staging_dir, new_dir)

def find_next_matching_block(old_chunk: bytes, new_file: BinaryIO) -> Optional[bytes]:
    new_chunk = new_file.read(BLOCK_SIZE)
    while new_chunk:
        if old_chunk == new_chunk:
            return new_chunk
        new_chunk = new_file.read(BLOCK_SIZE)
    return None


def main():
    # Set the paths for the old and new directories
    old_dir = r"""C:/Users/lion-/Documents/Python/test/oldfolder"""
    reference_dir = r"""C:/Users/lion-/Documents/Python/test/referencefolder"""
    # Set the path for the destination directory where you want to apply the patch
    destination_dir = r"""C:/Users/lion-/Documents/Python/test/newfolder"""

    # Calculate the patch for the two directories
    patch = diff(old_dir, reference_dir)
    patch_size = sum(len(operation.data) for operation in patch.operations)
    print(f'Patch size: {patch_size} bytes')



    for operation in patch.operations:
        if operation.op_type  == OperationType.DATA:
            print(f'DATA operation with {len(operation.data)} bytes of data')
        elif operation.op_type == OperationType.BLOCK_RANGE:
            print(f'BLOCK_RANGE operation with file_index={operation.file_index}, block_index={operation.block_index}, and block_span={operation.block_span}')

    # Apply the patch to the destination directory
    apply_patch(patch, old_dir, destination_dir)
    print("Patch applied successfully")

main()
