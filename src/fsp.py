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

#
# Description: Fast EBS Direct API Client inspired by awslabs/coldsnap.
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Minimum requirements: 4 vCPU, 8GB RAM.
# Recommended:          8 vCPU, 32GB RAM, dedicated network bandwidth (5Gbps min).
#
# The memory requirements depends on Snapshot size. A fully allocated 16TiB
# snapshot uses 12.4 GiB RAM for the block index, and 10-16 GiB for the
# parallel copy process. If the script crashes due to OOM, you can reduce
# the copy memory requirement by reducing NUM_JOBS at the expense of performance.
#
# Benchmarked download speed vs. instance type **with** EBS VPC Endpoint:
# =========== x86  Intel =============
# m6i.xlarge:    447 MB/s     - min recommended  $0.192/hr (Sep 2021)
# m6i.2xlarge:   456 MB/s     - recommended      $0.384/hr (Sep 2021)
# m6i.4xlarge:   468 MB/s
# m6i.16xlarge:  480 MB/s     - max tested       $3.072/hr (Sep 2021)
# =========== Graviton 2 =============
# c6gn.xlarge:   422 MB/s     - min recommended  $0.173/hr. (Sep 2021)
# c6gn.2xlarge:  460 MB/s     - recommended      $0.346/hr. (Sep 2021)
# c6gn.16xlarge: 466 MB/s     - max tested       $2.760/hr. (Sep 2021)


import json
import boto3
import hashlib
import numpy as np
import os
import io
import sys
import time
import math
import zstandard
import platform
from base64 import b64encode, urlsafe_b64encode
from joblib import Parallel, delayed
from multiprocessing import Manager
from urllib.error import HTTPError
from botocore.exceptions import ClientError

# Import project scoped vars
from singleton import SingletonClass #Project Scoped Global Vars

singleton = SingletonClass()

# Global Constants
CHUNK_SIZE = 1024 * 512
MEGABYTE = 1024 * 1024
GIGABYTE = MEGABYTE * 1024
KNOWN_SPARSE_CHECKSUM = "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE="

# Source for Atomic Counter: http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing
class Counter(object):
    def __init__(self, manager, init_val=0):
        self.val = manager.Value("i", init_val)
        self.lock = manager.Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value


# Description:      Wrapper around ebs.get_snapshot_block() with retry logic.
# Data path:        EBS Snapshot -> EBS Direct API -> Local Memory
# Input worker:     EBS Client
# Input data:       N/A
# Input metadata:   Snapshot ID (string), BlockIndex, BlockToken
# Output:           EBS Direct API Response that contains CHUNK_SIZE worth of data
#
def try_get_block(ebs, snapshot_id, block_index, block_token):
    response = None
    retry_count = 0
    while response is None:
        try:
            response = ebs.get_snapshot_block(
                SnapshotId=snapshot_id, BlockIndex=block_index, BlockToken=block_token
            )
            continue
        except Exception as e:
            # We catch all errors here, but mostly it'll be API throttle events. 
            # We retry indefinitely on network interruptions, and only alert for second retry.
            # First-time throttle events happen fairly regularly so we ignore them.
            # TODO: Implement abort according to API best practices.
            error_code = e.response['Error']['Code']
            retry_count += 1  
            if (retry_count > 1): 
                log_snapshot_block_exception(block_token, retry_count, error_code, "GetSnapshotBlock")
            pass
    return response


# Description:      Wrapper around boto3 ebs.put_snapshot_block() with retry logic.
# Data path:        Local Memory -> EBS Direct API -> EBS Snapshot
# Input worker:     EBS Client
# Input data:       CHUNK_SIZE worth of bytes
# Input metadata:   Snapshot ID (string), BlockIndex, calculated SHA256 Checksum of data, atomic counter that we increment on success
# Output:           EBS Direct API Response
#
def try_put_block(ebs, block, snap_id, data, checksum, count):
    response = None
    retry_count = 0
    if checksum != KNOWN_SPARSE_CHECKSUM or singleton.FULL_COPY:  # Known sparse block checksum we can skip
        while response is None:
            try:
                response = ebs.put_snapshot_block(
                    SnapshotId=snap_id,
                    BlockIndex=block,
                    BlockData=data,
                    DataLength=CHUNK_SIZE,
                    Checksum=checksum,
                    ChecksumAlgorithm='SHA256'
                )
                continue
            except Exception as e:
                error_code = e.response['Error']['Code']
                retry_count += 1
                if retry_count > 1:
                    log_snapshot_block_exception(block, retry_count, error_code, "PutSnapshotBlock")
                pass
        count.increment()
    return response


# Description:      Helper function to provide helpful error messages for common issues.
#                   See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/error-retries.html
#                   Only GetSnapshotBlock and PutSnapshotBlock can throw a RequestThrottledException, 
#                   so we handle them separately and provide helpful pointers to the right service quotas.
def log_snapshot_block_exception(block, retry_count, error_code, operation):
    if operation:
        if operation == "GetSnapshotBlock" or operation == "PutSnapshotBlock":
            limit_level = ""
            if error_code == "ThrottlingException": limit_level = "account"
            elif error_code == "RequestThrottledException": limit_level = "snapshot"
            print ( block, "exceeded", 
                    operation, "requests per", 
                    limit_level, "limit", 
                    retry_count, "times, retrying. See quota", 
                    singleton.AWS_SERVICE_QUOTAS[operation, error_code])
            return
        if error_code == "AccessDeniedException":
            print (block, "failed", operation, retry_count, "times, aborting.", error_code, ". Please verify your IAM permissions." )
            sys.exit(77)
    # Fall through in case we hit unanticipated error code
    print (block, "failed", operation, retry_count, "times, retrying.", error_code)


# Description:      Helper function to write a block to a file at the right offset.
# Data path:        Local Memory -> File
# Input worker:     N/A
# Input data:       CHUNK_SIZE worth of bytes
# Input metadata:   Filename, Block Metadata containing Offset
# Output:           N/A
#
def write_block_to_file(file, block, data):
    with os.fdopen(os.open(file, os.O_WRONLY), "rb+") as f: # On Windows, we can write to a raw disk, but can't create or read.
        f.seek(block["BlockIndex"]*CHUNK_SIZE)
        f.write(data)
        f.flush()
        f.close()

# Description:      Helper function to verify received checksum with received data.
# Data path:        N/A
# Input worker:     N/A
# Input data:       CHUNK_SIZE worth of bytes
# Input metadata:   received checksum, Block Metadata
# Output:           Bool
#
def verify_checksum(received_checksum, block, data):
    h = hashlib.sha256()
    h.update(data)
    calculated_checksum = b64encode(h.digest()).decode()
    if received_checksum == calculated_checksum:
        return True
    else:
        print (f'Checksum verify for chunk {block} failed, retrying: {received_checksum} != {calculated_checksum}')
        return False


# Get a Snapshot Block, verify Checksum and write it to a file.
# Data Path: Local Memory (from try_get_block()) -> File / Block Device
def get_block(block, ebs, files, snapshot_id):
    resp = try_get_block(ebs, snapshot_id, block["BlockIndex"], block["BlockToken"])
    data = resp["BlockData"].read()
    if (
        resp["Checksum"] != KNOWN_SPARSE_CHECKSUM or singleton.FULL_COPY
    ): ## Known sparse block checksum we can skip if allowed
        if verify_checksum(resp["Checksum"], block, data):
            for file in files:
                write_block_to_file(file, block, data)
        else:
            get_block(block, ebs, files, snapshot_id)  # We retry indefinitely on checksum failure.


# Get a Changed Block, verify Checksum and write it at the right offset.
# Data Path: Local Memory (from try_get_block()) -> File / Block Device
def get_changed_block(block, ebs, files, snapshot_id_one, snapshot_id_two):
    resp = None
    if "SecondBlockToken" in block:
        resp = try_get_block(ebs, snapshot_id_two, block["BlockIndex"], block["SecondBlockToken"])
    else:
        resp = try_get_block(ebs, snapshot_id_one, block["BlockIndex"], block["FirstBlockToken"])
    data = resp["BlockData"].read()
    # For a changed block, we **don't** want to skip sparse blocks, since we want to overwrite non-sparse with sparse if that happens.
    if verify_checksum(resp["Checksum"], block, data):
        for file in files:
            write_block_to_file(file, block, data)
    else:
        get_changed_block(block, ebs, files, snapshot_id_one, snapshot_id_two)  # We retry indefinitely on checksum failure.

# Read a Block locally, try to upload it.
# Data Path: Local File / Block Device -> Memory -> EBS Direct API (via try_put_block()) -> EBS Snapshot
def put_block_from_file(block, ebs, snap_id, OUTFILE, count):
    block = int(block)
    with os.fdopen(os.open(OUTFILE, os.O_RDONLY | os.O_NONBLOCK), "rb+") as f:
        f.seek((block) * CHUNK_SIZE)
        data = f.read(CHUNK_SIZE)
        if not data:
            return
        data = data.ljust(CHUNK_SIZE, b"\0")
        checksum = b64encode(hashlib.sha256(data).digest()).decode()
        try_put_block(ebs, block, snap_id, data, checksum, count)


# Read a Block locally, try to upload it to multiple destinations in parallel.
# Data Path: Local File / Block Device -> Memory -> EBS Direct APIs (via try_put_block()) -> EBS Snapshots
def put_block_from_file_fanout(block, source, f, ebsclient_snaps):
    block = int(block)
    with os.fdopen(os.open(source, os.O_RDONLY | os.O_NONBLOCK), "rb+") as f:
        f.seek((block) * CHUNK_SIZE)
        data = f.read(CHUNK_SIZE)
        if not data:
            return
        data = data.ljust(CHUNK_SIZE, b"\0")
        checksum = b64encode(hashlib.sha256(data).digest()).decode()
        with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel3:
            parallel3(delayed(try_put_block)(
                ebsclient_snaps[ebsclient_snap]["client"],
                block,
                ebsclient_snaps[ebsclient_snap]["snapshot"]["SnapshotId"],
                data,
                checksum,
                ebsclient_snaps[ebsclient_snap]["count"]
            ) 
            for ebsclient_snap in ebsclient_snaps
        )


# Read a Snapshot from S3 in parallel.
# Data Path: S3 -> Local
def get_blocks_s3(array, snapshot_prefix):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION)  # we spawn a client per snapshot segment
    session=boto3.Session(profile_name=singleton.AWS_S3_PROFILE)
    s3 = session.client(
        "s3",
        region_name=singleton.AWS_ORIGIN_REGION,
        endpoint_url=singleton.AWS_S3_ENDPOINT_URL
    )
    with Parallel(n_jobs=singleton.NUM_JOBS) as parallel2:
        parallel2(
            delayed(get_block_s3)(block, ebs, s3, snapshot_prefix) for block in array
        )


# Copy Segments to S3 in parallel.
# Data Path:  -> S3
def put_segments_to_s3(snapshot_id, array, volume_size, s3bucket):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION)  # we spawn a client per snapshot segment
    session=boto3.Session(profile_name=singleton.AWS_S3_PROFILE)
    s3 = session.client(
        "s3",
        region_name=singleton.AWS_DEST_REGION,
        endpoint_url=singleton.AWS_S3_ENDPOINT_URL
    )
    h = hashlib.sha256()
    data = bytearray()
    offset = array[0]["BlockIndex"]
    for block in array:
        resp = try_get_block(ebs, snapshot_id, block["BlockIndex"], block["BlockToken"])
        data += resp["BlockData"].read()
    h.update(data)
    s3.put_object(
        Body=zstandard.compress(data, 1),
        Bucket=s3bucket, Key="{}.{}/{}.{}.{}.zstd".format(snapshot_id,
            volume_size,
            offset,
            urlsafe_b64encode(h.digest()).decode(), len(data) // CHUNK_SIZE
        )
    )

# Copy Segments to S3 in parallel.
# Data Path: -> S3
def put_segments_fanout(array, source, f, ebsclient_snaps):
    with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel2:
        parallel2(
            delayed(put_block_from_file_fanout)(block, source, f, ebsclient_snaps) 
            for block in array
        )

# Get a Segment from S3, uncompress, disassemble into Blocks, copy to EBS Snapshot.
# Data Path: S3 -> Local Memory -> EBS Snapshot (via try_put_block())
def get_segment_from_s3(object, snap, count):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION) # we spawn a client per snapshot segment
    session=boto3.Session(profile_name=singleton.AWS_S3_PROFILE)
    s3 = session.client(
        "s3",
        region_name=singleton.AWS_ORIGIN_REGION,
        endpoint_url=singleton.AWS_S3_ENDPOINT_URL
    )
    h = hashlib.sha256()
    response = s3.get_object(Bucket=singleton.S3_BUCKET, Key=object["Key"])
    name = object["Key"].split("/")[1].split(".") # Name format: snapshot_id.volsize/offset.checksum.length.compressalgo
    if name[3] == "zstd":
        data = zstandard.decompress(response["body"].read())
        h.update(data)
        if urlsafe_b64encode(h.digest()).decode() == name[1]:
            for i in range(int(name[2])):
                chunk = data[CHUNK_SIZE * i : CHUNK_SIZE * (i + 1)]
                h.update(chunk)
                checksum = b64encode(hashlib.sha256(chunk).digest()).decode()
                try_put_block(ebs, int(name[0]) + i, snap, chunk, checksum, count)

# Put a single Block to S3.
# Data Path: -> S3
def get_block_s3(block, ebs, s3, snapshot_prefix):
    h = hashlib.sha256()
    resp = try_get_block(ebs, snapshot_prefix, block["BlockIndex"], block["BlockToken"])
    data = resp["BlockToken"].read()
    checksum = resp["Checksum"]
    h.update(data)
    chksum = b64encode(h.digest()).decode()
    if checksum != KNOWN_SPARSE_CHECKSUM or singleton.FULL_COPY:  # Known sparse block checksum we can skip
        if chksum == checksum:
            s3.put_object(
                Body=data,
                Bucket=singleton.S3_BUCKET,
                Key="{}/{}.{}".format(
                    snapshot_prefix, block["BlockIndex"], h.hexdigest()
                )
            )
        else:
            print(f'Checksum verify for chunk {block} failed, retrying: {block} {checksum} {chksum}')
            get_block_s3(block,ebs, s3, snapshot_prefix) # We retry indefinitely on checksum failure.
    else:
        s3.put_object(
            Body="",
            Bucket=singleton.S3_BUCKET,
            Key="{}/{}.{}".format(snapshot_prefix, block["BlockIndex"], h.hexdigest())
        )

# Wrapper around get_block() that parallelizes individual get_block() retrievals.
# Data Path:
def get_blocks(array, files, snapshot_id):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION) # we spawn a client per snapshot segment
    with Parallel(n_jobs=singleton.NUM_JOBS) as parallel2:
        parallel2(
            delayed(get_block)(
                block, ebs, files, snapshot_id
            )
            for block in array
        )


# Wrapper around get_changed_block() that parallelizes individual get_changed_block() retrievals.
# Data Path:
def get_changed_blocks(array, files, snapshot_id_one, snapshot_id_two):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION) # we spawn a client per snapshot segment
    with Parallel(n_jobs=singleton.NUM_JOBS) as parallel2:
        parallel2(
            delayed(get_changed_block)(
                block, ebs, files, snapshot_id_one, snapshot_id_two
            )
            for block in array
        )


# Makes sure that files or device paths can be opened for writing and seeking.
# Data Path: N/A
def validate_file_paths(files):
    for file in files:
        try:
            if platform.system() == "Windows":
                os.fdopen(os.open(file, os.O_WRONLY), "rb+") # Windows doesn't allow O_CREAT on a PhysicalDrive
            else: 
                os.fdopen(os.open(file, os.O_WRONLY | os.O_CREAT), "rb+")
        except io.UnsupportedOperation:
            print ("ERROR:", file, "cannot be opened for writing or is not seekable. Please verify your file paths.\nIf you are using a device path to write to a raw volume, make sure to use /dev/nvmeXn1 and not /dev/nvmeX.")
            raise SystemExit

# Makes sure that files or device paths are readable.
# Data Path: N/A
def validate_file_paths_read(files):
    for file in files:
        try:
            os.fdopen(os.open(file, os.O_RDONLY), "rb+")
        except io.UnsupportedOperation:
            print (f"ERROR: {file} cannot be read from. Please verify your file paths.\n"
                + "If you are using a device path to read from to a raw volume, make sure to use /dev/nvmeXn1 and not /dev/nvmeX.")
            raise SystemExit


# Wrapper that parallelizes copying blocks between EBS Snapshots.
# Data Path: EBS Snapshot -> Direct API -> Local Memory -> Direct API 2 -> EBS Snapshot 2
def copy_blocks_to_snap(command, snapshot, array, snap, count):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION) # we spawn a client per snapshot segment
    ebs2 = boto3.client("ebs", region_name=singleton.AWS_DEST_REGION) # Using separate client for upload. This will allow cross-region/account copies.
    with Parallel(n_jobs=singleton.NUM_JOBS) as parallel2:
        parallel2(
            delayed(copy_block_to_snap)(
                command, snapshot, block, ebs, ebs2, snap, count
            )
            for block in array
        )

# Copy individual block from Source EBS Snapshot to Destination EBS Snapshot.
# Data Path: EBS Snaphot -> Direct API -> Local Memory -> Direct API 2 -> EBS Snapshot 2
def copy_block_to_snap(command, snapshot, block, ebs, ebs2, snap, count):
    if command == "copy":
        resp = try_get_block(ebs, snapshot, block["BlockIndex"], block["BlockToken"])
    elif command == "sync":
        resp = try_get_block(ebs, snapshot, block["BlockIndex"], block["SecondBlockToken"])
    if "BlockToken" in resp:
        data = resp["BlockToken"].read()
        checksum = b64encode(hashlib.sha256(data).digest()).decode()
        try_put_block(ebs2, block["BlockIndex"], snap["SnapshotId"], data, checksum, count)

# Wrapper around put_block_from_file() that parallelizes individual block uploads.
# Data path: File / Device -> EBS Direct API -> EBS Snapshot
def put_blocks(array, snap_id, OUTFILE, count):
    ebs = boto3.client("ebs", region_name=singleton.AWS_DEST_REGION)
    with Parallel(n_jobs=singleton.NUM_JOBS) as parallel2:
        parallel2(
            delayed(put_block_from_file)(
                block, ebs, snap_id, OUTFILE, count
            ) 
            for block in array
        )

# Core logic for combining Blocks into larger Segments for S3 Upload.
# Data Path: N/A, operates on a block map and doesn't touch data.
def chunk_and_align(array, gap=1, offset=64):
    result = []
    segment = []
    for item in array:
        if len(segment) == 0:
            segment.append(item)
        elif item["BlockIndex"]-segment[-1]["BlockIndex"] == gap and item["BlockIndex"] % offset != 0:
            segment.append(item)
        else:
            result.append(segment)
            segment = []
            segment.append(item)
    return result

# Get Block Metadata from an EBS snapshot.
# Metadata Path: EBS Snapshot -> Direct API -> Local Memory
def retrieve_snapshot_blocks(snapshot_id):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION)
    blocks = []
    response = ebs.list_snapshot_blocks(SnapshotId=snapshot_id)
    blocks = response['Blocks']
    while 'NextToken' in response:
        response = ebs.list_snapshot_blocks(SnapshotId=snapshot_id, NextToken = response['NextToken'])
        blocks.extend(response['Blocks'])
    return blocks


# Get Block Metadata from a diff of two Snapshots.
# Metadata Path: EBS Snapshot -> EBS Direct API -> Local Memory
def retrieve_differential_snapshot_blocks(snapshot_id_one, snapshot_id_two):
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION)
    response = ebs.list_changed_blocks(FirstSnapshotId=snapshot_id_one, SecondSnapshotId=snapshot_id_two)
    blocks = response["ChangedBlocks"]
    while "NextToken" in response:
        response = ebs.list_changed_blocks(
            FirstSnapshotId=snapshot_id_one,
            SecondSnapshotId=snapshot_id_two,
            NextToken=response['NextToken']
        )
        blocks.extend(response['ChangedBlocks'])
    return blocks


# Validate whether we can read from an EBS Snapshot - i.e. is it Completed?
# Data Path: N/A
def validate_snapshot(snapshot_id, region=singleton.AWS_ORIGIN_REGION): #Return if fsp can use the snapshot. exit otherwise
    valid = True
    try:
        ec2 = boto3.client("ec2", region_name=region)
        response = ec2.describe_snapshots(SnapshotIds=[snapshot_id])["Snapshots"][0]
        if not(response["Progress"] == "100%" and response["State"] == "completed"):
            print("Snapshot has yet to complete. :%s", response["Progress"])
            valid = False
    except ec2.exceptions as e:
        print(e)
        valid = False
    if valid == False:
        print("\nExiting")
        sys.exit(1)  # Exit code for invalid parameters. Script cannot run
    return valid


# Verify that we can READ from and WRITE to an S3 Bucket.
# Will fail if user doesn't have permissions to the bucket, if bucket doesn't exist, or if the permissions are incorrect.
def validate_s3_bucket(region, check_is_read, check_is_write): #Return if user has all required permissions on the bucket. Otherwise Invalid
    valid = True
    try:
        session=boto3.Session(profile_name=singleton.AWS_S3_PROFILE)
        s3 = session.client("s3", region_name=region, endpoint_url=singleton.AWS_S3_ENDPOINT_URL)
        try:
            response = s3.get_bucket_acl(Bucket=singleton.S3_BUCKET)["Grants"]
        except ClientError as e:  # Some S3 implementations don't support GetBucketAcl(), in that case ignore and hope we can continue.
            return
        found = False
        for grant in response:
            if grant["Grantee"]["ID"] == singleton.AWS_CANONICAL_USER_ID:
                found = True
                if grant["Permission"] == "FULL_CONTROL":
                    break  # Valid!
                if check_is_read == True and not grant["Permission"] == "READ":
                    print(f"s3 bucket {singleton.S3_BUCKET} does not not have read permissions for user {singleton.AWS_CANONICAL_USER_ID}")
                    valid = False
                    break
                if check_is_write == True and not grant["Permission"] == "WRITE":
                    print(f"s3 bucket {singleton.S3_BUCKET} does not not have write permissions for user {singleton.AWS_CANONICAL_USER_ID}")
                    valid = False
                    break
        if found == False:
            valid = False
    except s3.exceptions as e:
        print(e)
        valid = False
    except HTTPError as e:
        print(f"The bucket {singleton.S3_BUCKET} does not exist in AWS account {singleton.AWS_ACCOUNT_ID} for region {region}")
        valid = False
    except Exception as e:
        print("Could not validate s3 bucket")
        valid = False
    if valid == False:
        print("\nExiting")
        sys.exit(1)  # Exit code for invalid parameters. Script cannot run
    return valid


"""
The Functions below are wrappers exposed to main.py to be called after dependency checking and arg parsing
Each function below will use the functions above with parallelization to complete the intended action

Each function below follows the following format:
    1. Parameter Validation
    2. Execute Function
    3. Make relevant output and report time
"""


def list(snapshot_id):
    validate_snapshot(snapshot_id)
    start_time = time.perf_counter()
    blocks = retrieve_snapshot_blocks(snapshot_id)
    print('Snapshot', snapshot_id, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")

def diff(snapshot_id_one, snapshot_id_two):
    validate_snapshot(snapshot_id_one)
    validate_snapshot(snapshot_id_two)
    start_time = time.perf_counter()
    blocks = retrieve_differential_snapshot_blocks(snapshot_id_one, snapshot_id_two)
    print('Changes between', snapshot_id_one, 'and', snapshot_id_two, 'contain', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")

def download(snapshot_id, file_path):
    validate_snapshot(snapshot_id)
    files = []
    files.append(file_path)
    validate_file_paths(files)
    start_time = time.perf_counter()
    blocks = retrieve_snapshot_blocks(snapshot_id)
    print('Snapshot', snapshot_id, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")
    split = np.array_split(blocks, singleton.NUM_JOBS)
    start_time = time.perf_counter()
    num_blocks = len(blocks)
    print(files)
    with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
        parallel(delayed(get_blocks)(array, files, snapshot_id) for array in split)
    print('download took',round(time.perf_counter() - start_time, 2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - start_time), 2), 'bytes/sec.')

def deltadownload(snapshot_id_one, snapshot_id_two, file_path):
    validate_snapshot(snapshot_id_one)
    validate_snapshot(snapshot_id_two)
    files = []
    files.append(file_path)
    validate_file_paths(files)
    start_time = time.perf_counter()
    blocks = retrieve_differential_snapshot_blocks(snapshot_id_one, snapshot_id_two)
    split = np.array_split(blocks, singleton.NUM_JOBS)
    num_blocks = len(blocks)
    print('Changes between', snapshot_id_one, 'and', snapshot_id_two, 'contain', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")
    print(files)
    with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
        parallel(
            delayed(get_changed_blocks)(array, files, snapshot_id_one, snapshot_id_two)
            for array in split
        )  # retrieve the blocks of snapshot_one missing in snapshot_two
    print('deltadownload took',round(time.perf_counter() - start_time,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - start_time),2), 'bytes/sec.')

def upload(file_path, parent_snapshot_id):
    files = []
    files.append(file_path)
    validate_file_paths_read(files)
    start_time = time.perf_counter()
    ebs = boto3.client("ebs", region_name=singleton.AWS_ORIGIN_REGION)
    with os.fdopen(os.open(file_path, os.O_RDONLY | os.O_NONBLOCK), "rb+") as f: #! Warning: these file permissions could cause problems on windows
        f.seek(0, os.SEEK_END)
        size = f.tell()
        gbsize = math.ceil(size / GIGABYTE)
        chunks = size // CHUNK_SIZE
        split = np.array_split(range(chunks), singleton.NUM_JOBS)
        count = Counter(Manager(), 0)
        print("Size of", file_path, "is", size, "bytes and", chunks, "chunks")
        if parent_snapshot_id is None:
            snap = ebs.start_snapshot(VolumeSize=gbsize, Description="Uploaded by fsp.py from "+file_path)
        else:
            snap = ebs.start_snapshot(VolumeSize=gbsize, Description="Uploaded by fsp.py from "+file_path, ParentSnapshotId=parent_snapshot_id)
        with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
            parallel(
                delayed(put_blocks)(array, snap["SnapshotId"], file_path, count) 
                for array in split
            )
        ebs.complete_snapshot(SnapshotId=snap["SnapshotId"], ChangedBlocksCount=count.value())
        print(file_path,'took',round(time.perf_counter() - start_time,2), 'seconds at', round(CHUNK_SIZE * count.value() / (time.perf_counter() - start_time),2), 'bytes/sec.')
        print('Total chunks uploaded', count.value())
        print('Use the upload functionality at your own risk. Works on my machine...')
        print(snap["SnapshotId"]) # Always print Snapshot ID last, for easy | tail -1

def copy(snapshot_id):
    validate_snapshot(snapshot_id)
    start_time = time.perf_counter()
    blocks = retrieve_snapshot_blocks(snapshot_id)
    print('Snapshot', snapshot_id, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")
    split = np.array_split(blocks, singleton.NUM_JOBS)
    start_time = time.perf_counter()
    num_blocks = len(blocks)
    ec2 = boto3.client("ec2", region_name=singleton.AWS_ORIGIN_REGION)
    ebs2 = boto3.client("ebs", region_name=singleton.AWS_DEST_REGION) # Using separate client for upload. This will allow cross-region/account copies.
    gbsize = ec2.describe_snapshots(SnapshotIds=[snapshot_id,],)["Snapshots"][0]["VolumeSize"]
    count = Counter(Manager(), 0)
    snap = ebs2.start_snapshot(VolumeSize=gbsize, Description='Copied by fsp.py from '+snapshot_id)
    with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
        parallel(
            delayed(copy_blocks_to_snap)('copy', snapshot_id, array, snap, count)
            for array in split
        )
    print('copy took',round(time.perf_counter() - start_time,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - start_time),2), 'bytes/sec.')
    ebs2.complete_snapshot(SnapshotId=snap["SnapshotId"], ChangedBlocksCount=count.value())
    print(snap["SnapshotId"])

def sync(snapshot_id_one, snapshot_id_two, destination_snapshot):
    validate_snapshot(snapshot_id_one)
    validate_snapshot(snapshot_id_two)
    validate_snapshot(destination_snapshot, region=singleton.AWS_DEST_REGION)
    start_time = time.perf_counter()
    blocks = retrieve_differential_snapshot_blocks(snapshot_id_one, snapshot_id_two)
    print('Changes between', snapshot_id_one, 'and', snapshot_id_two, 'contain', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")
    split = np.array_split(blocks, singleton.NUM_JOBS)
    start_time = time.perf_counter()
    num_blocks = len(blocks)
    ec2 = boto3.client("ec2", region_name=singleton.AWS_ORIGIN_REGION)
    ebs = boto3.client("ebs", region_name=singleton.AWS_DEST_REGION)
    gbsize = ec2.describe_snapshots(SnapshotIds=[snapshot_id_one,],)["Snapshots"][0]["VolumeSize"]
    count = Counter(Manager(), 0)
    snap = ebs.start_snapshot(ParentSnapshotId=destination_snapshot, VolumeSize=gbsize, Description='Copied delta by fsp.py from '+snapshot_id_one+'to'+snapshot_id_two)
    print(snap["SnapshotId"])
    with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
        parallel(
            delayed(copy_blocks_to_snap)('sync', snapshot_id_two, array, snap, count)
            for array in split
        )
    print('sync took',round(time.perf_counter() - start_time,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - start_time),2), 'bytes/sec.')
    ebs.complete_snapshot(SnapshotId=snap["SnapshotId"], ChangedBlocksCount=count.value())

def movetos3(snapshot_id):
    validate_snapshot(snapshot_id)
    validate_s3_bucket(singleton.AWS_DEST_REGION, False, True)
    start_time = time.perf_counter()
    blocks = retrieve_snapshot_blocks(snapshot_id)
    print('Snapshot', snapshot_id, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")
    start_time = time.perf_counter()
    num_blocks = len(blocks)
    ec2 = boto3.client("ec2", region_name=singleton.AWS_ORIGIN_REGION)
    gbsize = ec2.describe_snapshots(SnapshotIds=[snapshot_id,],)["Snapshots"][0]["VolumeSize"]
    with Parallel(n_jobs=128, require="sharedmem") as parallel:
        #parallel(delayed(get_blocks_s3)(array, snapshot_id) for array in split)
        parallel(
            delayed(put_segments_to_s3)(snapshot_id, array, gbsize, singleton.S3_BUCKET)
            for array in chunk_and_align(blocks, 1, 64)
        )
    print('movetos3 took',round(time.perf_counter() - start_time,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - start_time),2), 'bytes/sec.')

def getfroms3(snapshot_prefix):
    validate_s3_bucket(singleton.AWS_DEST_REGION, True, False)
    start_time = time.perf_counter()
    session=boto3.Session(profile_name=singleton.AWS_S3_PROFILE)
    s3 = session.client("s3", region_name=singleton.AWS_ORIGIN_REGION, endpoint_url=singleton.AWS_S3_ENDPOINT_URL)
    ebs = boto3.client("ebs", region_name=singleton.AWS_DEST_REGION)
    response = s3.list_objects_v2(Bucket=singleton.S3_BUCKET, Prefix=snapshot_prefix)
    objects = response["Contents"]
    count = Counter(Manager(), 0)
    while "NextContinuationToken" in response:
        response = s3.list_objects_v2(Bucket=singleton.S3_BUCKET, Prefix=snapshot_prefix, ContinuationToken = response["NextContinuationToken"])
        objects.extend(response["Contents"])
    if len(objects) == 0:
        print("No snapshots found for prefix %s in bucket %s" % (snapshot_prefix, singleton.S3_BUCKET))
    snap = ebs.start_snapshot(VolumeSize=int(objects[0]["Key"].split("/")[0].split(".")[1]), Description='Restored by fsp.py from S3://'+singleton.S3_BUCKET+'/'+objects[0]["Key"].split("/")[0])
    with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
        parallel(
            delayed(get_segment_from_s3)(object, snap["SnapshotId"], count) 
            for object in objects
        )
    print('getfroms3 took',round(time.perf_counter() - start_time,2), 'seconds at', round(CHUNK_SIZE * count.value() / (time.perf_counter() - start_time),2), 'bytes/sec.')
    ebs.complete_snapshot(SnapshotId=snap["SnapshotId"], ChangedBlocksCount=count.value())
    print(snap["SnapshotId"])

def multiclone(snapshot_id, infile):
    validate_snapshot(snapshot_id)
    files = []
    with open(infile, "r") as f:
        files = f.read().splitlines()
    validate_file_paths(files)
    start_time = time.perf_counter()
    blocks = retrieve_snapshot_blocks(snapshot_id)
    print('Snapshot', snapshot_id, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - start_time,2), "seconds.")
    split = np.array_split(blocks, singleton.NUM_JOBS)  # Separate the snapshot into segments to be processed in parallel
    start_time = time.perf_counter()
    num_blocks = len(blocks)
    print(files)
    with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
        parallel(
            delayed(get_blocks)(array, files, snapshot_id) for array in split
        )
    print('multiclone took',round(time.perf_counter() - start_time,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - start_time),2), 'bytes/sec.')

def fanout(device_path, destination_regions):
    files = []
    files.append(device_path)
    validate_file_paths_read(files)
    # Note destination_regions was validated while singleton was being configured (Near origin and destination regions validation)
    ebs_clients = {}
    snaps = {}
    ebsclient_snaps = {}
    with os.fdopen(os.open(device_path, os.O_RDONLY | os.O_NONBLOCK), "rb+") as f: #! Warning: these file permissions could cause problems on windows
        f.seek(0, os.SEEK_END)
        size = f.tell()
        gbsize = math.ceil(size / GIGABYTE)
        chunks = size // CHUNK_SIZE
        split = np.array_split(range(chunks), singleton.NUM_JOBS)
        print("Size of", device_path, "is", size, "bytes and", chunks, "chunks. Aligning snapshot to", gbsize, "GiB boundary.")
        for region in destination_regions:
            ebs_clients[region] = boto3.client("ebs", region_name=region)
            snaps[region] = ebs_clients[region].start_snapshot(VolumeSize=gbsize, Description="Uploaded by fsp.py from "+ device_path)
            ebsclient_snaps[region]={
                "client":ebs_clients[region],
                "snapshot":snaps[region],
                "count":Counter(Manager(), 0)
            }
        print("Spawned", len(ebsclient_snaps), "EBS Clients and started a snapshot in each region.")
        with Parallel(n_jobs=singleton.NUM_JOBS, require="sharedmem") as parallel:
            parallel(
                delayed(put_segments_fanout)(array, device_path, f, ebsclient_snaps) 
                for array in split
            )
        output = {}
        for region in ebsclient_snaps:
            ebs = ebsclient_snaps[region]["client"]
            snapshot_id = ebsclient_snaps[region]["snapshot"]["SnapshotId"]
            count = ebsclient_snaps[region]["count"]
            ebs.complete_snapshot(SnapshotId=snapshot_id, ChangedBlocksCount=count.value())
            output[region] = snapshot_id
        print(json.dumps(output)) #record all regions and their snapshots in a key-value pair format for easy log tail
