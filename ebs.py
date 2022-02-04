#!/usr/bin/env python3
#
# Description: Fast EBS Direct API Client inspired by awslabs/coldsnap.
# Author: Kirill Davydychev, kdavyd@amazon.com
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Minimum requirements: 4 vCPU, 8GB RAM.
# Recommended:          8 vCPU, 16GB RAM, dedicated network bandwidth (5Gbps min).
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
import sys
import time
import threading
from base64 import b64encode
from joblib import Parallel, delayed
from joblib import wrap_non_picklable_objects
from joblib.externals.loky import set_loky_pickler
from multiprocessing import Manager, Value, Lock

AWS_REGION = "us-east-1"
AWS_DEST_REGION = "us-east-1"
CHUNK_SIZE = 1024 * 512
MEGABYTE = 1024 * 1024
GIGABYTE = MEGABYTE * 1024
if AWS_REGION == AWS_DEST_REGION:
	NUM_JOBS = 16 # Snapshot gets split into N chunks, each of which is processed using N threads. Total complexity N^2.
else:
	NUM_JOBS = 27 # Increase concurrency for cross-region copies for better bandwidth.
                      # The value of 27 has been chosen because we appear to load-balance across 3 endpoints, so makes sense to use power of 3. 
                      # In testing, I was able to get 450MB/s between N.Virginia and Australia/Tokyo.
FULL_COPY = False     # By default, we skip known zeroed blocks. Enable this if you need a full copy of incrementals.
print(NUM_JOBS)
parser = argparse.ArgumentParser(description='EBS DirectAPI Client.')
parser.add_argument('command', help='copy, diff, download, list, sync, upload, clone, multiclone', type=str)
parser.add_argument('snapshot', help='snapshot id', type=str, )
parser.add_argument('outfile', help='Output file for download or second snapshot for diff.', nargs="?")
parser.add_argument('destsnap', help='Destination snapshot for delta sync.', nargs="?")
args = parser.parse_args()
COMMAND = args.command
SNAPSHOT_ID = args.snapshot
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

def get_block(block, ebs, files):
    h = hashlib.sha256()
    resp = None
    count = 0
    while resp == None:
        try:
            resp = ebs.get_snapshot_block(SnapshotId=SNAPSHOT_ID, BlockIndex=block['BlockIndex'], BlockToken = block['BlockToken'])
            continue
        except:
            count += 1    # We catch all errors here, mostly it'll be API throttle events so we just assume. In theory should work with network interruptions as well.
            if count > 1: # Only alert for second retry, but keep trying indefinitely. First-time throttle events happen fairly regularly.
                print (block, "throttled by API", count, "times, retrying.")
            pass
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

def put_block(block, ebs, snap_id, OUTFILE, count):
    block = int(block)
    with os.fdopen(os.open(OUTFILE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
        f.seek((block) * CHUNK_SIZE)
        data = f.read(CHUNK_SIZE)
        if not data:
            return
        data = data.ljust(CHUNK_SIZE, b'\0')
        checksum = b64encode(hashlib.sha256(data).digest()).decode()
        chksum = resp['Checksum'];
        resp = None
        c = 0
        if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=" or FULL_COPY: ## Known sparse block checksum we can skip
            while resp == None:
                try:
                    resp = ebs.put_snapshot_block(SnapshotId=snap_id, BlockIndex=block, BlockData=data, DataLength=CHUNK_SIZE, Checksum=chksum, ChecksumAlgorithm='SHA256')
                    continue
                except:
                    count += 1
                    if count > 1:
                        print (block, "throttled by API", count, "times, retrying.")
                    pass
            count.increment()

def get_blocks_s3(array):
    ebs = boto3.client('ebs') # we spawn a client per snapshot segment
    s3 = boto3.client('s3')
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(get_block_s3)(block, ebs, s3) for block in array)

def get_block_s3(block, ebs, s3):
    h = hashlib.sha256()
    resp = None
    count = 0
    while resp == None:
        try:
            resp = ebs.get_snapshot_block(SnapshotId=SNAPSHOT_ID, BlockIndex=block['BlockIndex'], BlockToken = block['BlockToken'])
            continue
        except:
            count += 1    # We catch all errors here, mostly it'll be API throttle events so we just assume. In theory should work with network interruptions as well.
            if count > 1: # Only alert for second retry, but keep trying indefinitely. First-time throttle events happen fairly regularly.
                print (block, "throttled by API", count, "times, retrying.")
            pass
    data = resp['BlockData'].read();
    checksum = resp['Checksum'];
    h.update(data)
    chksum = b64encode(h.digest()).decode()
    if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=" or FULL_COPY: ## Known sparse block checksum we can skip
        if chksum == checksum:
            s3.put_object(Body=data, Bucket='kd-ebs-snaps', Key="{}/{}".format(SNAPSHOT_ID, block['BlockIndex']))
	    #with os.fdopen(os.open(OUTFILE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
                #f.seek(block['BlockIndex']*CHUNK_SIZE)
                #f.write(data)
                #f.flush()
                #f.close()
        else:
            print ('Checksum verify for chunk',block,'failed, retrying:', block, checksum, chksum)
            get_block(block,ebs) # We retry indefinitely on checksum failure.
    else:
        s3.put_object(Body="", Bucket='kd-ebs-snaps', Key="{}/{}".format(SNAPSHOT_ID, block['BlockIndex']))

def get_blocks(array, files):
    ebs = boto3.client('ebs') # we spawn a client per snapshot segment
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(get_block)(block, ebs, files) for block in array)

def copy_blocks_to_snap(array, snap, count):
    ebs = boto3.client('ebs') # we spawn a client per snapshot segment
    ebs2 = boto3.client('ebs', region_name=AWS_DEST_REGION) # Using separate client for upload. This will allow cross-region/account copies.
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(copy_block_to_snap)(block, ebs, ebs2, snap, count) for block in array)

def copy_block_to_snap(block, ebs, ebs2, snap, count):
    h = hashlib.sha256()
    resp = None
    retry_count = 0
    while resp == None:
        try:
            if COMMAND in 'copy':
                resp = ebs.get_snapshot_block(SnapshotId=SNAPSHOT_ID, BlockIndex=block['BlockIndex'], BlockToken = block['BlockToken'])
            elif COMMAND in 'sync':
                resp = ebs.get_snapshot_block(SnapshotId=OUTFILE, BlockIndex=block['BlockIndex'], BlockToken = block['SecondBlockToken'])
            continue
        except:
            retry_count += 1    # We catch all errors here, mostly it'll be API throttle events so we just assume. In theory should work with network interruptions as well.
            if retry_count > 1: # Only alert for second retry, but keep trying indefinitely. First-time throttle events happen fairly regularly.
                print (block, "throttled by API", count, "times, retrying.")
            pass
    data = resp['BlockData'].read();
    checksum = b64encode(hashlib.sha256(data).digest()).decode()
    retry_count = 0
    resp = None
    if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=" or FULL_COPY: ## Known sparse block checksum we can skip
        while resp == None:
            try:
                resp = ebs2.put_snapshot_block(SnapshotId=snap['SnapshotId'], BlockIndex=block['BlockIndex'], BlockData=data, DataLength=CHUNK_SIZE, Checksum=checksum, ChecksumAlgorithm='SHA256')
                continue
            except:
                retry_count += 1
                if retry_count > 1:
                    print (block, "throttled by API", count, "times, retrying.")
                pass
        count.increment()

def put_blocks(array, snap_id, OUTFILE, count):
    ebs = boto3.client('ebs')
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(put_block)(block, ebs, snap_id, OUTFILE, count) for block in array)

def main():
    starttime = time.perf_counter()
    ebs = boto3.client('ebs')
    ebs2 = boto3.client('ebs', region_name=AWS_DEST_REGION) # Using separate client for upload. This will allow cross-region/account copies.
    if COMMAND in ['diff', 'sync']: # Compute delta between two snapshots and build a list of chunks.
        response = ebs.list_changed_blocks(FirstSnapshotId=SNAPSHOT_ID, SecondSnapshotId=OUTFILE)
        blocks = response['ChangedBlocks']
        while 'NextToken' in response:
            response = ebs.list_changed_blocks(FirstSnapshotId=SNAPSHOT_ID, SecondSnapshotId=OUTFILE, NextToken = response['NextToken'])
            blocks.extend(response['ChangedBlocks']) 
        print ('Changes between', SNAPSHOT_ID,'and',OUTFILE,'contain', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
    if COMMAND in ['download', 'list', 'movetos3', 'multiclone', 'copy']: # Compute size of individual snapshot and build a list of chunks.
        response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID)
        blocks = response['Blocks']
        while 'NextToken' in response:
            response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID, NextToken = response['NextToken'])
            blocks.extend(response['Blocks'])
        print ('Snapshot', SNAPSHOT_ID, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
    if COMMAND in 'download': # Download snapshot to a local file or raw device.
        starttime = time.perf_counter()
        files = []
        files.append(OUTFILE)
        print(files)
        split = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(get_blocks)(array, files) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * len(blocks) / (time.perf_counter() - starttime),2), 'bytes/sec.')
    if COMMAND in 'multiclone': # Download snapshot to multiple files in parallel. Especially useful for cloning volumes - works with raw device paths.
        starttime = time.perf_counter()
        files = []
        with open(OUTFILE, "r") as f:
            files = f.read().splitlines()
        print(files)
        split = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(get_blocks)(array, files) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * len(blocks) / (time.perf_counter() - starttime),2), 'bytes/sec.')
    if COMMAND in 'copy': # Copy to new snapshot.
        starttime = time.perf_counter()
        split = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
        ec2 = boto3.client("ec2")
        gbsize = ec2.describe_snapshots(SnapshotIds=[SNAPSHOT_ID,],)['Snapshots'][0]['VolumeSize']
        print (gbsize)
        manager = Manager()
        count = Counter(manager, 0)
        snap = ebs2.start_snapshot(VolumeSize=gbsize, Description='Copied from '+SNAPSHOT_ID)
        print(snap['SnapshotId'])
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(copy_blocks_to_snap)(array, snap, count) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * len(blocks) / (time.perf_counter() - starttime),2), 'bytes/sec.')
        print (count.value(), len(blocks))
        print (ebs2.complete_snapshot(SnapshotId=snap['SnapshotId'], ChangedBlocksCount=count.value()))
    if COMMAND in 'sync': # Synchronize deltas between SnapA and SnapB to SnapC.
        starttime = time.perf_counter()
        split = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
        ec2 = boto3.client("ec2")
        gbsize = ec2.describe_snapshots(SnapshotIds=[SNAPSHOT_ID,],)['Snapshots'][0]['VolumeSize']
        print (gbsize)
        manager = Manager()
        count = Counter(manager, 0)
        snap = ebs.start_snapshot(ParentSnapshotId=DESTSNAP, VolumeSize=gbsize, Description='Copied delta from '+SNAPSHOT_ID+'to'+OUTFILE)
        print(snap['SnapshotId'])
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(copy_blocks_to_snap)(array, snap, count) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * len(blocks) / (time.perf_counter() - starttime),2), 'bytes/sec.')
        print (count.value(), len(blocks))
        print (ebs.complete_snapshot(SnapshotId=snap['SnapshotId'], ChangedBlocksCount=count.value()))
    if COMMAND in 'movetos3': # Experimental - copy individual chunks to S3 as objects. There is currently no logic to restore from S3.
        starttime = time.perf_counter()
        split = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(get_blocks_s3)(array) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * len(blocks) / (time.perf_counter() - starttime),2), 'bytes/sec.')
    if COMMAND in 'upload': # Upload from file to snapshot(s).
        with os.fdopen(os.open(OUTFILE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            gbsize = size // GIGABYTE
            chunks = size // CHUNK_SIZE
            blocks = range(chunks)
            manager = Manager()
            count = Counter(manager, 0)
            split = np.array_split(blocks,NUM_JOBS)
            print("Size of file is ", size, "bytes and ", chunks, "chunks")
            snap = ebs.start_snapshot(VolumeSize=gbsize, Description=OUTFILE )
            snap_id = snap['SnapshotId']
            with Parallel(n_jobs=NUM_JOBS) as parallel:
                parallel(delayed(put_blocks)(array, snap_id, OUTFILE, count) for array in split)
            ebs.complete_snapshot(SnapshotId=snap_id, ChangedBlocksCount=count.value())
            print(COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * count.value() / (time.perf_counter() - starttime),2), 'bytes/sec. for', snap_id)
            print('Total chunks uploaded', count.value())
                
        # TODO Upload logic. Upload sources from file to new snapshot, clone sources directly from snapshot to one/multiple **new** volumes. 
        # Primary use case for upload: re-thin zeroed blocks in a snapshot.
        # Use case for clone: Same as download, but takes in a list and is multi-destination.
            print ("Use the upload functionality at your own risk. Works on my machine...")



if __name__ == "__main__":
    main()

