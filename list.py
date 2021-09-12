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
from base64 import b64encode
from joblib import Parallel, delayed
from joblib import wrap_non_picklable_objects
from joblib.externals.loky import set_loky_pickler

AWS_REGION = "us-east-1"
CHUNK_SIZE = 1024 * 512
MEGABYTE = 1024 * 1024
GIGABYTE = MEGABYTE * 1024
NUM_JOBS = 16 # Snapshot gets split into N chunks, each of which is processed using N threads. Total complexity N^2.

parser = argparse.ArgumentParser(description='EBS DirectAPI Client.')
parser.add_argument('command', help='download, list, diff, upload, clone', type=str)
parser.add_argument('snapshot', help='snapshot id', type=str, )
parser.add_argument('outfile', help='Output file for download or second snapshot for diff.', nargs="?")
args = parser.parse_args()
COMMAND = args.command
SNAPSHOT_ID = args.snapshot
OUTFILE = args.outfile

def get_block(block, ebs):
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
    if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=": ## Known sparse block checksum we can skip
        if chksum == checksum:
            with os.fdopen(os.open(OUTFILE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
                f.seek(block['BlockIndex']*CHUNK_SIZE)
                f.write(data)
                f.flush()
                f.close()
        else: 
            print ('Checksum verify for chunk',block,'failed, retrying:', block, checksum, chksum)
            get_block(block,ebs) # We retry indefinitely on checksum failure.

def get_blocks(array):
    ebs = boto3.client('ebs') # we spawn a client per snapshot segment
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(get_block)(block, ebs) for block in array)

def main():
    starttime = time.perf_counter()
    ebs = boto3.client('ebs')
    if COMMAND in 'diff':
        response = ebs.list_changed_blocks(FirstSnapshotId=SNAPSHOT_ID, SecondSnapshotId=OUTFILE)
        blocks = response['Blocks']
        while 'NextToken' in response:
            response = ebs.list_changed_blocks(FirstSnapshotId=SNAPSHOT_ID, SecondSnapshotId=OUTFILE, NextToken = response['NextToken'])
            blocks.extend(response['Blocks'])
        print ('Changes between', SNAPSHOT_ID,'and',OUTFILE,'contain', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
    if COMMAND in ['download', 'list']: 
        #session = boto3.session.Session(region_name=AWS_REGION)
        #ec2 = session.resource('ec2')
        response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID)
        blocks = response['Blocks']
        while 'NextToken' in response:
            response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID, NextToken = response['NextToken'])
            blocks.extend(response['Blocks'])
        print ('Snapshot', SNAPSHOT_ID, 'contains', len(blocks), 'chunks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
    if COMMAND in 'download':
        starttime = time.perf_counter()
        split = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(get_blocks)(array) for array in split)
        print (COMMAND,'took',round(time.perf_counter() - starttime,2), 'seconds at', round(CHUNK_SIZE * len(blocks) / (time.perf_counter() - starttime),2), 'bytes/sec.')
    if COMMAND in ['upload', 'clone']:
        # TODO Upload logic. Upload sources from file to new snapshot, clone sources directly from snapshot to one/multiple **new** volumes. 
        # Primary use case for upload: re-thin zeroed blocks in a snapshot.
        # Use case for clone: Same as download, but takes in a list and is multi-destination.
        print ("Not supported yet.")

if __name__ == "__main__":
    main()