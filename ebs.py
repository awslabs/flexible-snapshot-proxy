#!/usr/bin/env python3
#
# Description: Fast EBS Direct API Client inspired by awslabs/coldsnap.
# Author: Kirill Davydychev, kdavyd@amazon.com
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


import argparse
import boto3
import hashlib
import numpy as np
import os
import io
import sys
import time
import threading
import math
import zstandard
from base64 import b64encode, urlsafe_b64encode
from joblib import Parallel, delayed
from joblib import wrap_non_picklable_objects
from joblib.externals.loky import set_loky_pickler
from multiprocessing import Manager, Value, Lock

if boto3.session.Session().region_name is None:
    AWS_REGION = "us-east-1" # we assume us-east-1 if region is not configured in aws cli; please change to your actual region.
else:
    AWS_REGION = boto3.session.Session().region_name
AWS_DEST_REGION = "us-east-1"
S3_BUCKET = 'ownsrob-snapshots'
CHUNK_SIZE = 1024 * 512
MEGABYTE = 1024 * 1024
GIGABYTE = MEGABYTE * 1024
FULL_COPY = False# By default, we skip known zeroed blocks. Set to True if you need a full copy of incrementals.
if AWS_REGION == AWS_DEST_REGION:
	NUM_JOBS = 16 # Snapshot gets split into N chunks, each of which is processed using N threads. Total complexity N^2.
else:
	NUM_JOBS = 27 # Increase concurrency for cross-region copies for better bandwidth.
                  # The value of 27 has been chosen because we appear to load-balance across 3 endpoints, so makes sense to use power of 3. 
                  # In testing, I was able to get 450MB/s between N.Virginia and Australia/Tokyo.

parser = argparse.ArgumentParser(description='EBS DirectAPI Client.')
parser.add_argument('command', help='copy, diff, download, list, sync, upload, clone, multiclone', type=str)
parser.add_argument('snapshot', help='snapshot id', type=str, )
parser.add_argument('outfile', help='Output file for download or second snapshot for diff.', nargs="?")
parser.add_argument('destsnap', help='Destination snapshot for delta sync.', nargs="?")
args = parser.parse_args()
COMMAND = args.command
SOURCE = args.snapshot
OUTFILE = args.outfile
DESTSNAP = args.destsnap

# http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing
class Counter(object):
    def __init__(self, manager, initval=0):
        self.val = manager.Value('i', initval)
        self.lock = manager.Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value

def try_get_block(ebs, snapshot_id, blockindex, blocktoken):
    resp = None
    retry_count = 0
    while resp == None:
        try:
            resp = ebs.get_snapshot_block(SnapshotId=snapshot_id, BlockIndex=blockindex, BlockToken=blocktoken)
            continue
        except:
            retry_count += 1    # We catch all errors here, mostly it'll be API throttle events so we just assume. In theory should work with network interruptions as well.
            if retry_count > 1: # Only alert for second retry, but keep trying indefinitely. First-time throttle events happen fairly regularly.
                print (blocktoken, "throttled by API", retry_count, "times, retrying.")
            pass
    return resp
    
def try_put_block(ebs, block, snap_id, data, checksum, count):
    resp = None
    retry_count = 0
    if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=" or FULL_COPY: ## Known sparse block checksum we can skip
        while resp == None:
            try:
                resp = ebs.put_snapshot_block(SnapshotId=snap_id, BlockIndex=block, BlockData=data, DataLength=CHUNK_SIZE, Checksum=checksum, ChecksumAlgorithm='SHA256')
                continue
            except:
                retry_count += 1
                if retry_count > 1:
                    print (block, "throttled by API", retry_count, "times, retrying.")
                pass
        count.increment()
    return resp

def get_block(block, ebs, files):
    h = hashlib.sha256()
    resp = try_get_block(ebs, SOURCE, block['BlockIndex'], block['BlockToken'])
    data = resp['BlockData'].read();
    checksum = resp['Checksum'];
    h.update(data)
    chksum = b64encode(h.digest()).decode()
    if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=" or FULL_COPY: ## Known sparse block checksum we can skip
        if chksum == checksum:
            for file in files:
                with os.fdopen(os.open(file, os.O_RDWR | os.O_CREAT), 'rb+') as f:
                    f.seek(block['BlockIndex']*CHUNK_SIZE)
                    f.write(data)
                    f.flush()
                    f.close()
        else:
            print ('Checksum verify for chunk',block,'failed, retrying:', block, checksum, chksum)
            get_block(block,ebs,files) # We retry indefinitely on checksum failure.

def put_block_from_file(block, ebs, snap_id, OUTFILE, count):
    block = int(block)
    with os.fdopen(os.open(OUTFILE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
        f.seek((block) * CHUNK_SIZE)
        data = f.read(CHUNK_SIZE)
        if not data:
            return
        data = data.ljust(CHUNK_SIZE, b'\0')
        checksum = b64encode(hashlib.sha256(data).digest()).decode()
        try_put_block(ebs, block, snap_id, data, checksum, count)

def get_blocks_s3(array):
    ebs = boto3.client('ebs', region_name=AWS_REGION) # we spawn a client per snapshot segment
    s3 = boto3.client('s3', region_name=AWS_REGION)
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(get_block_s3)(block, ebs, s3) for block in array)

def put_segments_to_s3(array, volsize):
    ebs = boto3.client('ebs', region_name=AWS_REGION) # we spawn a client per snapshot segment
    s3 = boto3.client('s3', region_name=AWS_REGION)
    h = hashlib.sha256()
    data = bytearray()
    offset = array[0]['BlockIndex'];
    for block in array:
        resp = try_get_block(ebs, SOURCE, block['BlockIndex'], block['BlockToken'])
        data += resp['BlockData'].read()
    h.update(data)
    print(len(data))
    s3.put_object(Body=zstandard.compress(data, 1), Bucket=S3_BUCKET, Key="{}.{}/{}.{}.{}.zstd".format(SOURCE, volsize, offset, urlsafe_b64encode(h.digest()).decode(), len(data) // CHUNK_SIZE))

def get_segment_from_s3(object, snap, count):
    ebs = boto3.client('ebs', region_name=AWS_REGION) # we spawn a client per snapshot segment
    s3 = boto3.client('s3', region_name=AWS_REGION)
    h = hashlib.sha256()
    response = s3.get_object(Bucket=S3_BUCKET, Key=object['Key'])
    name = object['Key'].split("/")[1].split(".") # Name format: snapshot_id.volsize/offset.checksum.length.compressalgo
    if name[3] == 'zstd':
        data = zstandard.decompress(response['Body'].read())
        h.update(data)
        if urlsafe_b64encode(h.digest()).decode() == name[1]:
            for i in range(int(name[2])):
                chunk = data[CHUNK_SIZE*i : CHUNK_SIZE*(i+1)]
                h.update(chunk)
                checksum = b64encode(hashlib.sha256(chunk).digest()).decode()
                try_put_block(ebs, int(name[0])+i, snap, chunk, checksum, count)

def get_block_s3(block, ebs, s3):
    h = hashlib.sha256()
    resp = try_get_block(ebs, SOURCE, block['BlockIndex'], block['BlockToken'])
    data = resp['BlockData'].read();
    checksum = resp['Checksum'];
    h.update(data)
    chksum = b64encode(h.digest()).decode()
    if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=" or FULL_COPY: ## Known sparse block checksum we can skip
        if chksum == checksum:
            s3.put_object(Body=data, Bucket=S3_BUCKET, Key="{}/{}.{}".format(SOURCE, block['BlockIndex'], h.hexdigest()))
        else:
            print ('Checksum verify for chunk',block,'failed, retrying:', block, checksum, chksum)
            get_block_s3(block,ebs, s3) # We retry indefinitely on checksum failure.
    else:
        s3.put_object(Body="", Bucket=S3_BUCKET, Key="{}/{}.{}".format(SOURCE, block['BlockIndex'], h.hexdigest()))

def get_blocks(array, files):
    ebs = boto3.client('ebs', region_name=AWS_REGION) # we spawn a client per snapshot segment
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(get_block)(block, ebs, files) for block in array)
        
def validate_file_paths(files):
    for file in files:
        try:
            os.fdopen(os.open(file, os.O_RDWR | os.O_CREAT), 'rb+')
        except io.UnsupportedOperation:
            print ("ERROR:", file, "cannot be opened for writing or is not seekable. Please verify your file paths.\nIf you are using a device path to write to a raw volume, make sure to use /dev/nvmeXn1 and not /dev/nvmeX.")
            raise SystemExit

def copy_blocks_to_snap(array, snap, count):
    ebs = boto3.client('ebs', region_name=AWS_REGION) # we spawn a client per snapshot segment
    ebs2 = boto3.client('ebs', region_name=AWS_DEST_REGION) # Using separate client for upload. This will allow cross-region/account copies.
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(copy_block_to_snap)(block, ebs, ebs2, snap, count) for block in array)

def copy_block_to_snap(block, ebs, ebs2, snap, count):
    if COMMAND in 'copy':
        resp = try_get_block(ebs, SOURCE, block['BlockIndex'], block['BlockToken'])
    elif COMMAND in 'sync':
        resp = try_get_block(ebs, OUTFILE, block['BlockIndex'], block['SecondBlockToken'])
    data = resp['BlockData'].read();
    checksum = b64encode(hashlib.sha256(data).digest()).decode()
    try_put_block(ebs2, block['BlockIndex'], snap['SnapshotId'], data, checksum, count)

def put_blocks(array, snap_id, OUTFILE, count):
    ebs = boto3.client('ebs', region_name=AWS_REGION)
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(put_block_from_file)(block, ebs, snap_id, OUTFILE, count) for block in array)
        
def chunk_and_align(array, gap = 1, offset = 64):
    result = []
    segment = []
    for item in array:
        if len(segment) == 0:
            segment.append(item)
        elif item['BlockIndex']-segment[-1]['BlockIndex'] == gap and item['BlockIndex'] % offset != 0:
            segment.append(item)
        else:
            result.append(segment)
            segment = []
            segment.append(item)
    print(len(result))
    return result

def main():
    starttime = time.perf_counter()
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    ebs = boto3.client('ebs', region_name=AWS_REGION)
    ebs2 = boto3.client('ebs', region_name=AWS_DEST_REGION) # Using separate client for upload. This will allow cross-region/account copies.
    blocks = []
    if COMMAND in ['diff', 'sync']: # Compute delta between two snapshots and build a list of chunks.
        if OUTFILE != None:
            response = ebs.list_changed_blocks(FirstSnapshotId=SOURCE, SecondSnapshotId=OUTFILE)
            blocks = response['ChangedBlocks']
            while 'NextToken' in response:
                response = ebs.list_changed_blocks(FirstSnapshotId=SOURCE, SecondSnapshotId=OUTFILE, NextToken = response['NextToken'])
                blocks.extend(response['ChangedBlocks']) 
            print ('Changes between', SOURCE,'and',OUTFILE,'contain', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
        else:
            print("Second snapshot ID not specified. Reverting to list behavior.")
            response = ebs.list_snapshot_blocks(SnapshotId=SOURCE)
            blocks = response['Blocks']
            while 'NextToken' in response:
                response = ebs.list_snapshot_blocks(SnapshotId=SOURCE, NextToken = response['NextToken'])
                blocks.extend(response['Blocks']) 
            print ('Changes between None and', SOURCE,'contain', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
    if COMMAND in ['download', 'list', 'movetos3', 'multiclone', 'copy']: # Compute size of individual snapshot and build a list of chunks.
        response = ebs.list_snapshot_blocks(SnapshotId=SOURCE)
        blocks = response['Blocks']
        while 'NextToken' in response:
            response = ebs.list_snapshot_blocks(SnapshotId=SOURCE, NextToken = response['NextToken'])
            blocks.extend(response['Blocks'])
        print ('Snapshot', SOURCE, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
    split = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
    starttime = time.perf_counter()
    num_blocks = len(blocks)
    if COMMAND in 'download': # Download snapshot to a local file or raw device.
        files = []
        files.append(OUTFILE)
        print(files)
        validate_file_paths(files)
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(get_blocks)(array, files) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - starttime),2), 'bytes/sec.')
    if COMMAND in 'multiclone': # Download snapshot to multiple files in parallel. Especially useful for cloning volumes - works with raw device paths.
        files = []
        with open(OUTFILE, "r") as f:
            files = f.read().splitlines()
        print(files)
        validate_file_paths(files)
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(get_blocks)(array, files) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - starttime),2), 'bytes/sec.')
    if COMMAND in 'copy': # Copy to new snapshot.
        gbsize = ec2.describe_snapshots(SnapshotIds=[SOURCE,],)['Snapshots'][0]['VolumeSize']
        count = Counter(Manager(), 0)
        snap = ebs2.start_snapshot(VolumeSize=gbsize, Description='Copied by ebs.py from '+SOURCE)
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(copy_blocks_to_snap)(array, snap, count) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - starttime),2), 'bytes/sec.')
        ebs2.complete_snapshot(SnapshotId=snap['SnapshotId'], ChangedBlocksCount=count.value())
        print (snap['SnapshotId'])
    if COMMAND in 'sync': # Synchronize deltas between SnapA and SnapB to SnapC.
        gbsize = ec2.describe_snapshots(SnapshotIds=[SOURCE,],)['Snapshots'][0]['VolumeSize']
        count = Counter(Manager(), 0)
        snap = ebs.start_snapshot(ParentSnapshotId=DESTSNAP, VolumeSize=gbsize, Description='Copied delta by ebs.py from '+SOURCE+'to'+OUTFILE)
        print(snap['SnapshotId'])
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(copy_blocks_to_snap)(array, snap, count) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - starttime),2), 'bytes/sec.')
        ebs.complete_snapshot(SnapshotId=snap['SnapshotId'], ChangedBlocksCount=count.value())
    if COMMAND in 'movetos3': # Experimental - copy individual chunks to S3 as objects. There is currently no logic to restore from S3.
        # split = np.array_split(blocks,math.ceil(num_blocks/128)) # 64MB segments to potentially concatenate. A lower value impacts performance.
        gbsize = ec2.describe_snapshots(SnapshotIds=[SOURCE,],)['Snapshots'][0]['VolumeSize']
        with Parallel(n_jobs=128) as parallel:
            #parallel(delayed(get_blocks_s3)(array) for array in split)
            parallel(delayed(put_segments_to_s3)(array, gbsize) for array in chunk_and_align(blocks, 1, 64))
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * num_blocks / (time.perf_counter() - starttime),2), 'bytes/sec.')
    if COMMAND in 'getfroms3': # Experimental - restore snapshot from S3. 
        s3 = boto3.client('s3', region_name=AWS_REGION)
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=SOURCE)
        objects = response['Contents']
        count = Counter(Manager(), 0)
        while 'NextContinuationToken' in response:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=SOURCE, ContinuationToken = response['NextContinuationToken'])
            objects.extend(response['Contents'])
        snap = ebs.start_snapshot(VolumeSize= int(objects[0]['Key'].split("/")[0].split(".")[1]), Description='Restored by ebs.py from S3://'+S3_BUCKET+'/'+objects[0]['Key'].split("/")[0])
        with Parallel(n_jobs = NUM_JOBS) as parallel:
            parallel(delayed(get_segment_from_s3)(object, snap['SnapshotId'], count) for object in objects)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * count.value() / (time.perf_counter() - starttime),2), 'bytes/sec.')
        ebs.complete_snapshot(SnapshotId=snap['SnapshotId'], ChangedBlocksCount=count.value())
        print (snap['SnapshotId'])
    if COMMAND in 'upload': # Upload from file to snapshot(s).
        with os.fdopen(os.open(SOURCE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            gbsize = math.ceil(size / GIGABYTE)
            chunks = size // CHUNK_SIZE
            split = np.array_split(range(chunks),NUM_JOBS)
            count = Counter(Manager(), 0)
            print("Size of file is", size, "bytes and", chunks, "chunks")
            snap = ebs.start_snapshot(VolumeSize=gbsize, Description="Uploaded by ebs.py from "+SOURCE)
            with Parallel(n_jobs=NUM_JOBS) as parallel:
                parallel(delayed(put_blocks)(array, snap['SnapshotId'], SOURCE, count) for array in split)
            ebs.complete_snapshot(SnapshotId=snap['SnapshotId'], ChangedBlocksCount=count.value())
            print(COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * count.value() / (time.perf_counter() - starttime),2), 'bytes/sec.')
            print('Total chunks uploaded', count.value())
            print('Use the upload functionality at your own risk. Works on my machine...')
            print(snap['SnapshotId']) # Always print Snapshot ID last, for easy | tail -1

if __name__ == "__main__":
    main()

