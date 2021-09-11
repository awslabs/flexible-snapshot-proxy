import os
import sys
import boto3
from joblib.externals.loky import set_loky_pickler
#from joblib import parallel_backend
from joblib import Parallel, delayed
from joblib import wrap_non_picklable_objects
#import math
import hashlib
from base64 import b64encode
#import asyncio
#from random import randint
#from concurrent import futures
import numpy as np
import argparse
import time

AWS_REGION = "us-east-1"
SNAPSHOT_ID = "snap-0a0c91fa12de0526b"
OUTFILE = "snap.out"
CHUNK_SIZE = 1024 * 512
NUM_JOBS = 16 # Snapshot gets split into N chunks, each of which is processed using N threads. Total complexity N^2.
failed_blocks = []

parser = argparse.ArgumentParser(description='EBS DirectAPI Client.')
parser.add_argument('command', help='Download a snapshot to file.', type=str)
parser.add_argument('snapshot', help='Output file', type=str, )
parser.add_argument('outfile', nargs="?", help='Output file', )
args = parser.parse_args()
COMMAND = args.command
SNAPSHOT_ID = args.snapshot
OUTFILE = args.outfile
print (args.command)
print (args.snapshot)
print (args.outfile)


def get_block(block, ebs):
    h = hashlib.sha256()
#    print(block)
#   print("====")
#    print(block['BlockIndex'])
    resp = None
    count = 0
    while resp == None:
        try:
            resp = ebs.get_snapshot_block(SnapshotId=SNAPSHOT_ID, BlockIndex=block['BlockIndex'], BlockToken = block['BlockToken'])
            continue
        except:
            count += 1
            if count > 1:
                print (block, "throttled by API", count, "times.")
            pass
    data = resp['BlockData'].read();
    checksum = resp['Checksum'];
    h.update(data)
    chksum = b64encode(h.digest()).decode()
    if checksum != "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=": ## Known sparse block checksum
        if chksum == checksum:
#            print ("Verified checksum", checksum)
            with os.fdopen(os.open(OUTFILE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
                f.seek(block['BlockIndex']*CHUNK_SIZE)
                f.write(data)
                f.flush()
                f.close()
        else:
            print ('Checksum verify for block',block,'failed, retrying:', block, checksum, chksum)
            get_block(block,ebs)

def get_blocks(array):
    ebs = boto3.client('ebs') # we spawn a client per snapshot segment
    with Parallel(n_jobs=NUM_JOBS) as parallel2:
        parallel2(delayed(get_block)(block, ebs) for block in array)

def main():
    starttime = time.perf_counter()
    if COMMAND in ['download', 'list']: 
        #session = boto3.session.Session(region_name=AWS_REGION)
        #ec2 = session.resource('ec2')
        ebs = boto3.client('ebs')
        response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID)
        blocks = response['Blocks']
        while 'NextToken' in response:
            response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID, NextToken = response['NextToken'])
            blocks.extend(response['Blocks'])
            #print ("Added dict")
        print ('Snapshot', SNAPSHOT_ID, 'contains', len(blocks), 'blocks and', CHUNK_SIZE * len(blocks), 'bytes, took', round (time.perf_counter() - starttime,2), "seconds.")
    if COMMAND in 'download':
        starttime = time.perf_counter
        split4 = np.array_split(blocks,NUM_JOBS) # Separate the snapshot into segments to be processed in parallel
        with Parallel(n_jobs=NUM_JOBS) as parallel:
            parallel(delayed(get_blocks)(array) for array in split4)
        print ("Download failed ", len(failed_blocks), "blocks,", COMMAND,'took', round(time.perf_counter()-starttime,2), 'seconds at', round(float(len(blocks))/(time.perf_counter()-starttime),0), 'bytes/sec.')

if __name__ == "__main__":
    main()


    
#for i in range(16):
#    ebs_clients.append(boto3.client('ebs'))

#for block in blocks:
#    data = get_block(block)

#pool = multiprocessing.Pool(multiprocessing.cpu_count())
#pool.apply_async(get_block, [block for block in blocks])
#pool.close()
#pool.join()



#    f = open(OUTFILE,"a")
    
    

