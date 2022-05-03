import os
import sys
import boto3
import multiprocessing
from joblib import Parallel, delayed
import math
import hashlib
from base64 import b64encode
import asyncio
from random import randint

AWS_REGION = "us-east-1"
SNAPSHOT_ID = "snap-0a0c91fa12de0526b"
OUTFILE = "snap.out"
CHUNK_SIZE = 1024 * 512

session = boto3.session.Session(region_name=AWS_REGION)
ec2 = session.resource('ec2')
ebs = boto3.client('ebs')
ebs_clients = [];

response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID)
blocks = response['Blocks']
failed_blocks = []


while 'NextToken' in response:
    response = ebs.list_snapshot_blocks(SnapshotId=SNAPSHOT_ID, NextToken = response['NextToken'])
    blocks.extend(response['Blocks'])
    print ("Added dict")
    
print ('Snapshot', SNAPSHOT_ID, 'contains', len(blocks), 'blocks and', CHUNK_SIZE * len(blocks), 'bytes')

def get_block(block):
    h = hashlib.sha256()
    print(block)
    print("====")
    print(block['BlockIndex'])
    resp = ebs_clients[0].get_snapshot_block(SnapshotId=SNAPSHOT_ID, BlockIndex=block['BlockIndex'], BlockToken = block['BlockToken'])
    data = resp['BlockData'].read();
    checksum = resp['Checksum'];
    h.update(data)
    chksum = b64encode(h.digest()).decode()
    if checksum == "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=": ## Known sparse block checksum
        return 0
    if chksum == checksum:
        print ("Verified checksum", checksum)
        with os.fdopen(os.open(OUTFILE, os.O_RDWR | os.O_CREAT), 'rb+') as f:
            f.seek(block['BlockIndex']*CHUNK_SIZE)
            f.write(data)
            f.flush()
        return 0
    else:
        print ("Checksum verify failed: ", checksum, chksum)
        failed_blocks.add(block)
        return 0
    return 0
    
for i in range(16):
    ebs_clients.append(boto3.client('ebs'))

for block in blocks:
    data = get_block(block)

#Parallel(n_jobs=1)(get_block(block) for block in blocks)

print ("Failed ", len(failed_blocks), "blocks")

#    f = open(OUTFILE,"a")
    
    

