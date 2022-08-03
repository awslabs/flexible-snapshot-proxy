"""
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

import os
import subprocess
import sys
from time import sleep


'''The following sizes are tuneable constants. Sizes are in bytes unless otherwise specified.
'''
SECTOR_SIZE = 2 ** 9
GB_SIZE = 2 ** 30
BITS_IN_BYTE = 2 ** 3
INTEGER_SIZE_IN_BITS = 32
ENDIANNESS = "big"
PATH_TO_FSP_CLIENT = os.path.dirname(os.path.realpath(__file__)) + "/../src/main.py"


'''Functions to treat Disk as 2d array organized [index][offset]. Where index is the sector number, and offset is the byte offset from start of the index.
'''
def byte_offset_to_index(byte_offset):
    return byte_offset // SECTOR_SIZE

def index_to_byte_offset(index, offset = 0):
    return index * SECTOR_SIZE + offset

"""Creates a snapshot for testing

Tread Disk as an array indexed by sector number. 
Creates a pattern where each i index that belongs to pattern rules is written to with 32 bit integer i (the sector number).
Returns snapshot_id and metadata to recreate and check the pattern.

Args:
    size (int): size of disk in GB. Max is 5GB, Default is 1GB.
    start (int): first index (sector number) of disk where pattern can begin from, default is start of disk
    end (int): last index (sector number) of disk where pattern can exist, default is end of disk
    skip (int): every skip sectors will be written to with the sector number. 
        Writes occur to sectors on [start + offset, end] that match the pattern. Otherwise a 0 NULL value is written 
    offset (int): number of sectors to skip before starting pattern - an offset of the pattern (cant be more than skip)
        N.B. Offset is NOT where the data is written to within a sector! Data is always written to the first 32 bits of a sector.
    parent tuple(str, bool): (OPTIONAL!) tuple containing parent snapshot id and boolean indicating if this is an additive pattern. 
        If True then the new snapshot will contain the parent pattern with the new pattern laid overtop (additive)
        Else the snapshot will only contain only the new pattern specified in the function call (subtractive or mutation)
        N.B. This parameter was added with the use case of testing "sync" in mind!

Returns:
    JSON: {
        snapshot_id (string),
        parent_id (string),
        snapshot_size (int),
        "metadata": {
            start (int),
            end (int),
            skip (int),
            offset (int)
        }
    }
    OR None if parameters are not valid!
"""
def generate_pattern_snapshot(size = 1, start = 0, end = None, skip = 1, offset = 0, parent=None):
    DEVICE_SIZE = GB_SIZE * size

    if end is None:
        end = DEVICE_SIZE // SECTOR_SIZE

    # Param validation
    if ((size > 5 or size < 1) 
        or (end > DEVICE_SIZE // SECTOR_SIZE)
        or (offset >= skip)
        or size < 1
        or start > byte_offset_to_index(size * GB_SIZE)
        or end <= start + offset or offset < 0):
        return None

    with open("/tmp/zeroes", "w") as outfile:
        subprocess.run(["sudo", "head", "-c" , str(DEVICE_SIZE), "/dev/zero"], stdout=outfile)
    LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
    subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"], capture_output=True)

    if (not (parent is None)) and (parent[1] == True):
        while True:
            # create a snapshot by adding a pattern on-top
            bash_get_parent = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "download", parent[0], LOOP_FILE], capture_output=True)
            if bash_get_parent.returncode == 0:
                break
            else:
                print(".", end='', flush=True) # show the backoff in output (often EC2 API can view a snapshot ~1 min before EBS Direct API)
                sleep(3)

    with open(LOOP_FILE, 'wb') as loop:
        loop.seek(index_to_byte_offset(start), 1)
        loop.seek(index_to_byte_offset(offset), 1)
        # Preform writes for patterns on [start + offset, end] of Volume sectors
        while loop.tell() < ( index_to_byte_offset(end + skip + 1) - skip*SECTOR_SIZE ):

            advanced = 0
            sec_num = byte_offset_to_index(loop.tell())
            if ( sec_num % skip ) == (( start + offset ) % skip ):
                advanced += loop.write(sec_num.to_bytes(INTEGER_SIZE_IN_BITS, ENDIANNESS))

            if (index_to_byte_offset(skip) + loop.tell()) - advanced >= DEVICE_SIZE:
                break
            else:
                loop.seek(index_to_byte_offset(skip) - advanced, 1)

    while True:
        if parent is None:
            bash_upload = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "upload" , LOOP_FILE], capture_output=True)
        else:
            bash_upload = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "upload" , LOOP_FILE, "--parent_snapshot_id", parent[0]], capture_output=True)
        
        if bash_upload.returncode == 0: # todo error handle for correct exception (ValidationException)
            break
        else:
            print(".", end='', flush=True) # show the backoff in output (often EC2 API can view a snapshot ~1 min before EBS Direct API)
            sleep(3)

    lines = bash_upload.stdout.decode("utf-8").split("\n")
    snapshot_id = lines[4].strip()

    # Cleanup
    subprocess.run(["sudo", "losetup", "-d", LOOP_FILE])
    
    return {
        "snap": snapshot_id,
        "size": size,
        "metadata": {
            "start": start,
            "end": end,
            "skip": skip,
            "offset": offset,
        },
        "parent": parent,
    }

"""Validate that a snapshot matches all patterns specified to exist on it.

Checker will recreate Disk from a snapshot.
Then iterate through every sector of disk
    for each pattern metadata that is specified checker will validate that:
        -  pattern exists at that sector (sector number is on Disk at sector i)
        -  pattern does not exist (NULL - all 0s is on Disk at sector i) e.g. in the gap of a pattern

Args:
    snapshot_id (string): specifies the snapshot to retrieve and test patterns against
    size (int): size of a snapshot in GB
    patters (list): a list of metadata that is to used to check recreated patterns - only specify patterns that exist! Not ones that have 'dropped out'
    device_path (string): (OPTIONAL!) If the snapshot has already been created, specify the destination and function will skip retrieval step

Returns:
    boolean: True if all specified patterns exist on the snapshot, False otherwise 
"""
def check_pattern(snapshot_id, size, patterns, device_path=None):
    DEVICE_SIZE = GB_SIZE * size

    LOOP_FILE = device_path
    if device_path is None:
        with open("/tmp/zeroes", "w") as outfile:
            subprocess.run(["sudo", "head", "-c" , str(DEVICE_SIZE), "/dev/zero"], stdout=outfile)
        LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
        subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"])

        while True:
            bash_download = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "download", snapshot_id, LOOP_FILE], capture_output=True)
            if bash_download.returncode == 0:
                break

            print(".", end='', flush=True) # show the backoff in output (often EC2 API can view a snapshot ~1 min before EBS Direct API)
            sleep(3)
        
    counter = -1 # tracking which sector of disk loop is currently checking. Only increments by 1 sector at a time regardless of pattern 
    NULL = 0
    with open(LOOP_FILE, 'rb') as loop:
        loop.seek(0, 2)
        ENDING_OFFSET = loop.tell()

        loop.seek(0,0)
        while loop.tell() <= (ENDING_OFFSET - SECTOR_SIZE):
            counter += 1
            loop.seek(SECTOR_SIZE * counter, 0)

            int_as_binary = loop.read(INTEGER_SIZE_IN_BITS)
            sec_num = int.from_bytes(int_as_binary, ENDIANNESS)
            cur_index = byte_offset_to_index(loop.tell())

            hit = False
            for pattern_metadata in patterns:
                start = pattern_metadata["start"]
                end = pattern_metadata["end"]
                skip = pattern_metadata["skip"]
                offset = pattern_metadata["offset"]
                # Are we on a non-null sector of the pattern?
                if ((cur_index >= start and cur_index <= end)
                    and (( cur_index % skip ) == (( start + offset ) % skip ))):
                    if sec_num != counter:
                        print(f"WRONG: Read Sector Number as {sec_num}. Should be {byte_offset_to_index(loop.tell())}")
                        print(f"start = {start} ({index_to_byte_offset(start)}) end = {end} ({index_to_byte_offset(end)}) skip = {skip} ({index_to_byte_offset(skip)}) offset = {offset}")
                        print(f"expected = {byte_offset_to_index(loop.tell())} at {loop.tell() - INTEGER_SIZE_IN_BITS} byte_offset but got {sec_num}")
                        print(LOOP_FILE)
                        return False
                    hit = True

            # Otherwise (no patterns were applicable for this sector) verify that the data is null
            if hit == False:
                if sec_num != NULL:
                    print(f"WRONG: Read Sector Number as {sec_num}. Should be NULL - ({NULL})")
                    print(f"start = {start} end = {end} skip = {skip} offset = {offset} expected = 0 at {counter} sector but got {sec_num}")
                    return False
    
    return True

# Run this file to tests an end to end test snapshot creation and verification
if __name__ == "__main__":
    if os.getuid() != 0:
        print("MUST RUN AS SUPER USER (SUDO)")
        sys.exit(1)


    list_of_patterns = []
    pattern1 = generate_pattern_snapshot(size=1, start=0, end=None, skip=1, offset=0)

    list_of_patterns.append(pattern1["metadata"])
    snapshot = pattern1["snap"]
    gb_size = pattern1["size"]
    print(f"Check Disk Pattern: {check_pattern(snapshot, gb_size, list_of_patterns)}\n\n")
