import json
import os
import subprocess
import sys
from time import sleep


'''
The following sizes are all in bits
'''
SECTOR_SIZE = 2 ** 9
GB_SIZE = 2 ** 30
BITS_IN_BYTE = 2 ** 3
INTEGER_SIZE_IN_BITS = 32
ENDIANNESS = "big"
PATH_TO_FSP_CLIENT = "../src/main.py"


'''
Functions to treat Disk as 2d array organized [index][offset]. Where index is the sector number
'''
def byte_offset_to_index(byte_offset):
    return byte_offset // SECTOR_SIZE

def index_to_byte_offset(index, offset = 0):
    return index * SECTOR_SIZE + offset

"""
Creates a snapshot for testing

Tread Disk as an array indexed by sector number. 
Creates a pattern where each i index that belongs to pattern rules is written the 32 bit integer i (the sector number).
Returns snapshot_id and metadata to recreate and check the pattern.

Parameters:
size (int): size of disk in GB. Max is 5GB, Default is 1GB.
start (int): first index (sector number) of disk where pattern can begin from, default is start of disk
end (int): last index (sector number) of disk where pattern can exist, default is end of disk
skip (int): every skip sectors will be written to with sector number on [start + offset, end]. Otherwise a 0 NULL value is written 
offset (int): number of sectors to skip before starting pattern - an offset of the pattern (cant be more than skip)
    N.B. Offset is NOT where the data is written to within a sector!

Returns:
JSON: {
    snapshot_id (string),
    snapshot_size (int),
    "metadata": {
        start (int),
        end (int),
        skip (int),
        offset (int)
    }
OR None if parameters are not valid!
}
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
    # print(f"Loopback device {size} GB created successfully")

    # print(f"Starting Byte Offset: {index_to_byte_offset(start)} - Sector # {start}")
    # print(f"Ending Byte Offset: {DEVICE_SIZE} - Sector # {byte_offset_to_index(DEVICE_SIZE)}")

    # Preform Sector by Sector writes
    # print(f"\nWriting Pattern skip={skip} - Sector Numbers on [{start + offset}, {end}]")
    with open(LOOP_FILE, 'wb') as loop:
        loop.seek(index_to_byte_offset(start), 1)
        loop.seek(index_to_byte_offset(offset), 1)
        # Preform writes for patterns on [start + offset, end] of Volume sectors
        while loop.tell() < ( index_to_byte_offset(end + skip + 1) - skip*SECTOR_SIZE ):

            advanced = 0
            sec_num = byte_offset_to_index(loop.tell())
            if ( sec_num % skip ) == (( start + offset ) % skip ):
                advanced += loop.write(sec_num.to_bytes(INTEGER_SIZE_IN_BITS, ENDIANNESS))

            # next write sector
            if (index_to_byte_offset(skip) + loop.tell()) - advanced >= DEVICE_SIZE:
                break
            else:
                loop.seek(index_to_byte_offset(skip) - advanced, 1)
        # print(f"{(round( (index_to_byte_offset(end) - index_to_byte_offset(start) ) / DEVICE_SIZE , 3) * 100)}% of disk contains this pattern")

    # print("Uploading snapshot...")
    while True:
        if parent is None:
            bash_upload = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "upload" , LOOP_FILE], capture_output=True)
        else:
            bash_upload = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "upload" , LOOP_FILE, "--parent_snapshot_id", parent], capture_output=True)
        
        if bash_upload.returncode == 0: # todo error handle for correct exception (ValidationException)
            break
        else:
            print(".", end='', flush=True)
            sleep(3)

    lines = bash_upload.stdout.decode("utf-8").split("\n")
    snapshot_id = lines[4].strip()
    # print("snapshot:", snapshot_id)

    # print("Cleanup...")
    subprocess.run(["sudo", "losetup", "-d", LOOP_FILE])
    
    # Return snap_id with metadata on how it was constructed
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

"""
Validate that a snapshot matches all patterns it is a member of

Checker will recreate Disk from a snapshot.
Then iterate through every sector of disk
    for each pattern metadata that is specified checker will validate that:
        -  pattern exists at that sector (sector number is on Disk at sector i)
        -  pattern does not exist (NULL - all 0s is on Disk at sector i) e.g. in the gap of a pattern

Parameters:
snapshot_id (string): specifies the snapshot to retrieve
size (int): size of a snapshot in GB
patters (list): a list of metadata that is to used to check recreated patterns - only specify patterns that exist! Not ones that have 'dropped out'
"""
def check_pattern(snapshot_id, size, patterns):
    DEVICE_SIZE = GB_SIZE * size

    # Recreate Disk
    # print("Creating Loop Device...")
    with open("/tmp/zeroes", "w") as outfile:
        subprocess.run(["sudo", "head", "-c" , str(DEVICE_SIZE), "/dev/zero"], stdout=outfile)
    LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
    # print(subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"], capture_output=True))
    # print(f"0'ed Loopback device created successfully - {size} GB")

    with open(LOOP_FILE, 'rb') as loop:
        loop.seek(0, 0)
        STARTING_OFFSET = loop.tell()
        loop.seek(0, 2)
        ENDING_OFFSET = loop.tell()
    # print(f"\tStarting Byte Offset: {STARTING_OFFSET} - Sector # {byte_offset_to_index(STARTING_OFFSET)}")
    # print(f"\tEnding Byte Offset: {ENDING_OFFSET} - Sector # {byte_offset_to_index(ENDING_OFFSET)}")

    # print("Downloading snapshot\nBacking Off ")
    bash_download = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "download", snapshot_id, LOOP_FILE], capture_output=True)
    while True:
        if bash_download.returncode == 0:
            break
        # print(".", end='', flush=True)
        sleep(3)
        bash_download = subprocess.run(["sudo", "python3", PATH_TO_FSP_CLIENT, "download", snapshot_id, LOOP_FILE], capture_output=True)

    if bash_download.returncode != 0:
        # print("Error downloading snapshot")
        sys.exit(1)
    
    # print(f"\nPatterns to check on restored volume\n{json.dumps(patterns, indent=2)}")

    # print("\nChecking Patterns - Reading Sector Numbers")
    counter = -1
    NULL = 0
    with open(LOOP_FILE, 'rb') as loop:
        loop.seek(0,0)
        while loop.tell() <= (ENDING_OFFSET - SECTOR_SIZE):
            counter += 1
            loop.seek(SECTOR_SIZE * counter, 0)

            if loop.tell() % SECTOR_SIZE != 0:
                # print("WRONG: At Byte Offset: ", loop.tell())
                return False

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
                if ((cur_index >= start and cur_index <= end) # Case 1: This is a pattern where the sector number should exist
                    and (( cur_index % skip ) == (( start + offset ) % skip ))): #! Prove this condition is true
                    if sec_num != counter:
                        # print(f"WRONG (P): Read Sector Number as {sec_num}. Should be {byte_offset_to_index(loop.tell())}")
                        # print(f"start = {start} ({index_to_byte_offset(start)}) end = {end} ({index_to_byte_offset(end)}) skip = {skip} ({index_to_byte_offset(skip)}) offset = {offset}")
                        # print(f"expected = {byte_offset_to_index(loop.tell())} at {loop.tell() - INTEGER_SIZE_IN_BITS} byte_offset but got {sec_num}")
                        return False
                    hit = True

            # Otherwise verify that the data is null
            if hit == False:
                if sec_num != NULL:
                    # print(f"WRONG (P'): Read Sector Number as {sec_num}. Should be NULL - ({NULL})")
                    # print(f"start = {start} end = {end} skip = {skip} offset = {offset} expected = 0 at {counter} sector but got {sec_num}")
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
    # print(f"Generated Disk Pattern: {pattern1}")
    snapshot = pattern1["snap"]
    gb_size = pattern1["size"]
    # print(snapshot)

    print(f"Check Disk Pattern: {check_pattern(snapshot, gb_size, list_of_patterns)}\n\n")
