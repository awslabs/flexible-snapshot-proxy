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

import json
import unittest
import os
import boto3
import math
import yaml
import subprocess
from time import sleep
from shutil import rmtree

def read_configuration_file():
    with open(f"{os.path.dirname(os.path.realpath(__file__))}/config.yaml", 'r') as file:
        config = yaml.safe_load(file)
    config['PATH_TO_PROJECT_DIRECTORY'] = os.path.dirname(os.path.realpath(__file__)) + '/..'
    config['PATH_TO_TEST'] = os.path.dirname(os.path.realpath(__file__)) + '/'
    
    return config
    
#Used as a check in many scripts    
def compute_size_of_snapshot(snapshot_id): #snapshot_id -> [size in chunks, size in bytes]
        CHUNK_SIZE = 1024 * 512
    
        #Taken from original script. Purpose of this test this that expected behavior is still matched after refactoring
        blocks = []
        try:
            ebs = boto3.client('ebs')
            response = ebs.list_snapshot_blocks(SnapshotId=snapshot_id)
            blocks = response['Blocks']
            while 'NextToken' in response:
                response = ebs.list_snapshot_blocks(SnapshotId=snapshot_id, NextToken = response['NextToken'])
                blocks.extend(response['Blocks'])
        except Exception as e:
            print("AWS Error Message\n",e)
        
        return [len(blocks), CHUNK_SIZE * len(blocks)]
        
def compute_size_of_diff(snapshot_one, snapshot_two): #snapshot_id -> [size in chunks, size in bytes]
    CHUNK_SIZE = 1024 * 512
    
    #Taken from original script. Purpose of this test this that expected behavior is still matched after refactoring
    try:
        ebs = boto3.client('ebs')
        response = ebs.list_changed_blocks(FirstSnapshotId=snapshot_one, SecondSnapshotId=snapshot_two)
        blocks = response['ChangedBlocks']
        while 'NextToken' in response:
            response = ebs.list_changed_blocks(FirstSnapshotId=snapshot_one, SecondSnapshotId=snapshot_two, NextToken = response['NextToken'])
            blocks.extend(response['ChangedBlocks']) 
        return [len(blocks), CHUNK_SIZE * len(blocks)]
        
    except Exception as e:
        print("AWS Error Message\n", e)

class CanaryListSnapshot(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryListSnapshot, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['ORG_SNAP'] = testing_configurations['ami-snapshot']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        
    
    def small_test_list(self):
        
        #Run script to move see size of snapshot
        command = f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps list {self.TEST_PARAMETERS['ORG_SNAP']}"
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
        
        #retrieve the command result
        output = result.stdout.decode('utf-8')    
        output = output.split(',')[0] #Remove the time reporting
        
        size = compute_size_of_snapshot(self.TEST_PARAMETERS['ORG_SNAP'])    
        self.assertEqual(output, f"Snapshot {self.TEST_PARAMETERS['ORG_SNAP']} contains {size[0]} chunks and {size[1]} bytes", "Mismatch Expected Output")
        
    def tearDown(self):
        super(CanaryListSnapshot, self).tearDown()
            
    
class CanaryDownloadSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryDownloadSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshotId'] = testing_configurations['small-volume-snapshots']['full']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE'] = testing_configurations['small-volume-path']
        
    def small_test_download(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps download {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()
            
        output[0] = output[0].split(',')[0] #Remove the time reporting
        
        expected = compute_size_of_snapshot(self.TEST_PARAMETERS["snapshotId"])

        self.assertEqual(output[0], f"Snapshot {self.TEST_PARAMETERS['snapshotId']} contains {expected[0]} chunks and {expected[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], f"['{self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}']")
        
    def tearDown(self):
        super(CanaryDownloadSnapshots, self).tearDown()

class CanaryDeltadownloadSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryDeltadownloadSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshot1'] = testing_configurations['small-volume-snapshots']['full']
        self.TEST_PARAMETERS['snapshot2'] = testing_configurations['small-volume-snapshots']['half']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE'] = testing_configurations['small-volume-path']
        
    def small_test_deltadownload(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps deltadownload {self.TEST_PARAMETERS['snapshot1']} {self.TEST_PARAMETERS['snapshot2']} {self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()
            
        output[0] = output[0].split(',')[0] #Remove the time reporting
        
        expected = compute_size_of_diff(self.TEST_PARAMETERS['snapshot1'], self.TEST_PARAMETERS['snapshot2'])

        self.assertEqual(output[0], f"Changes between {self.TEST_PARAMETERS['snapshot1']} and {self.TEST_PARAMETERS['snapshot2']} contain {expected[0]} chunks and {expected[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], f"['{self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}']")
        
    def tearDown(self):
        super(CanaryDeltadownloadSnapshots, self).tearDown()
            
class CanaryUploadSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    CLASS_SCOPE_VARS = {}
    
    def setUp(self):
        super(CanaryUploadSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['UPLOAD_BLOCKS'] = testing_configurations['small-volume-path']
        
    def compute_expected_output(self):
        CHUNK_SIZE = 1024 * 512
        MEGABYTE = 1024 * 1024
        GIGABYTE = MEGABYTE * 1024
    
        #Taken from original script. Purpose of this test this that expected behavior is still matched after refactoring
        try:
            with os.fdopen(os.open(self.TEST_PARAMETERS['UPLOAD_BLOCKS'], os.O_RDWR | os.O_CREAT), 'rb+') as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                gbsize = math.ceil(size / GIGABYTE)
                chunks = size // CHUNK_SIZE
                return [size, chunks]
        except Exception as e:
            print("OS Error Message\n",e)

        
    def small_test_upload(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps upload {self.TEST_PARAMETERS['UPLOAD_BLOCKS']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[2].strip()
        self.CLASS_SCOPE_VARS['new_snapshotId'] = lines[4].strip()
        
        expected = self.compute_expected_output()

        self.assertEqual(output[0], f"Size of {self.TEST_PARAMETERS['UPLOAD_BLOCKS']} is {expected[0]} bytes and {expected[1]} chunks", "Script output is not expected\nNote: Test will fail is FULL_COPY = False. (By reducing size of snapshot expected output is too large)")
        self.assertEqual(output[1], f"Total chunks uploaded {expected[1]}", "Script output is not expected")
        
    def tearDown(self):
        super(CanaryUploadSnapshots, self).tearDown()
        
        try:
            ec2 = boto3.client('ec2')
            #Delete 'restore snapshot'
            ec2.delete_snapshot(
                SnapshotId=self.CLASS_SCOPE_VARS['new_snapshotId'],
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)
            
class CanaryCopySnapshot(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    CLASS_SCOPE_VARS = {}
    
    def setUp(self):
        super(CanaryCopySnapshot, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshotId'] = testing_configurations['ami-snapshot']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        
        
    def small_test_copy(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps copy {self.TEST_PARAMETERS['snapshotId']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ''
        lines = result.stdout.decode('utf-8').split('\n')
        output = lines[0].strip()
        self.CLASS_SCOPE_VARS['new_snapshotId'] = lines[2].strip()
            
        output = output.split(',')[0] #Remove the time reporting
        
        size = compute_size_of_snapshot(self.TEST_PARAMETERS['snapshotId'])

        self.assertEqual(output, f"Snapshot {self.TEST_PARAMETERS['snapshotId']} contains {size[0]} chunks and {size[1]} bytes", "Script output is not expected")
        
        #check that new snapshot exists
        response = None
        try:
            ec2 = boto3.client('ec2')
            response = ec2.describe_snapshots(
                SnapshotIds=[self.CLASS_SCOPE_VARS['new_snapshotId']],
                DryRun=False
            )
            
            
        except Exception as e:
            print("AWS Error Message\n", e)
            
        self.assertIsNotNone(response, "No Response!")
        self.assertTrue((response['Snapshots'][0]['State'] == 'completed' or response['Snapshots'][0]['State'] == 'pending'), "Copy had an error!")
        
        
    def tearDown(self):
        super(CanaryCopySnapshot, self).tearDown()
        
        try:
            ec2 = boto3.client('ec2')
            #Delete 'restore snapshot'
            ec2.delete_snapshot(
                SnapshotId=self.CLASS_SCOPE_VARS['new_snapshotId'],
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)
  
class CanaryDiffSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryDiffSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshotId_1'] = testing_configurations["small-volume-snapshots"]["half"]
        self.TEST_PARAMETERS['snapshotId_2'] = testing_configurations["small-volume-snapshots"]["full"]
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        
    def small_test_diff(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps diff {self.TEST_PARAMETERS['snapshotId_1']} {self.TEST_PARAMETERS['snapshotId_2']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ''
        lines = result.stdout.decode('utf-8').split('\n')
        output = lines[0].strip()
            
        output = output.split(',')[0] #Remove the time reporting
        
        size = compute_size_of_diff(self.TEST_PARAMETERS['snapshotId_1'], self.TEST_PARAMETERS['snapshotId_2'])

        self.assertEqual(output, f"Changes between {self.TEST_PARAMETERS['snapshotId_1']} and {self.TEST_PARAMETERS['snapshotId_2']} contain {size[0]} chunks and {size[1]} bytes", "Script output is not expected")
        
        
    def tearDown(self):
        super(CanaryDiffSnapshots, self).tearDown()
            
class CanarySyncSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    CLASS_SCOPE_VARS = {}
    
    def setUp(self):
        super(CanarySyncSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshotId_1'] = testing_configurations["small-volume-snapshots"]["half"]
        self.TEST_PARAMETERS['snapshotId_2'] = testing_configurations["small-volume-snapshots"]["full"]
        self.TEST_PARAMETERS['snapshotId_parent'] = testing_configurations["small-volume-snapshots"]["empty"]
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']

        
    def small_test_sync(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps sync {self.TEST_PARAMETERS['snapshotId_1']} {self.TEST_PARAMETERS['snapshotId_2']} {self.TEST_PARAMETERS['snapshotId_parent']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ''
        lines = result.stdout.decode('utf-8').split('\n')
        output = lines[0].strip()
        self.CLASS_SCOPE_VARS['new_snapshotId'] = lines[1].strip()
            
        output = output.split(',')[0] #Remove the time reporting
        
        size = compute_size_of_diff(self.TEST_PARAMETERS['snapshotId_1'], self.TEST_PARAMETERS['snapshotId_2'])

        self.assertEqual(output, f"Changes between {self.TEST_PARAMETERS['snapshotId_1']} and {self.TEST_PARAMETERS['snapshotId_2']} contain {size[0]} chunks and {size[1]} bytes", "Script output is not expected")
        
        #check that new snapshot exists
        response = None
        try:
            ec2 = boto3.client('ec2')
            response = ec2.describe_snapshots(
                SnapshotIds=[self.CLASS_SCOPE_VARS['new_snapshotId']],
                DryRun=False
            )
            
            
        except Exception as e:
            print("AWS Error Message\n", e)
            
        self.assertIsNotNone(response, f"No Response! Checking Snapshot {self.CLASS_SCOPE_VARS['new_snapshotId']} failed.")
        self.assertTrue((response['Snapshots'][0]['State'] == 'completed' or response['Snapshots'][0]['State'] == 'pending'), "Sync had an error!")
        
        
    def tearDown(self):
        super(CanarySyncSnapshots, self).tearDown()
        
        try:
            ec2 = boto3.client('ec2')
            #Delete 'restore snapshot'
            ec2.delete_snapshot(
                SnapshotId=self.CLASS_SCOPE_VARS['new_snapshotId'],
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)

class CanaryMultiCloneSnapshot(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryMultiCloneSnapshot, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshotId'] = testing_configurations["ami-snapshot"]
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_TEST'] + 'temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/test.txt", mode='a').close() #Create the file to be written to
        
    def small_test_multiclone(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps multiclone {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/test.txt"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()
            
        output[0] = output[0].split(',')[0] #Remove the time reporting
        
        size = compute_size_of_snapshot(self.TEST_PARAMETERS["snapshotId"])

        self.assertEqual(output[0], f"Snapshot {self.TEST_PARAMETERS['snapshotId']} contains {size[0]} chunks and {size[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], "[]") #todo will be fixed once script is functional. should print all destinations snapshot was written to
        
    def tearDown(self):
        super(CanaryMultiCloneSnapshot, self).tearDown()
        
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))

class CanaryFanoutSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    CLASS_SCOPE_VARS = {}
    
    def setUp(self):
        super(CanaryFanoutSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['UPLOAD_BLOCKS'] = testing_configurations['small-volume-path']
        self.TEST_PARAMETERS['MAX_RETRY'] = testing_configurations['max-retry-count']
        self.TEST_PARAMETERS['RETRY_BACKOFF'] = testing_configurations['default-retry-time']
        self.TEST_PARAMETERS['REGIONS_FILE'] = "regions.txt"
        regions = ['us-east-1', 'us-east-2','us-west-1', 'us-west-2']
        with open(f"{testing_configurations['PATH_TO_TEST']}/{self.TEST_PARAMETERS['REGIONS_FILE']}", 'w') as f:
            for r in regions:
                f.write(r + '\n')
        self.CLASS_SCOPE_VARS['REGION_LIST'] = regions
        
    def compute_expected_output(self):
        CHUNK_SIZE = 1024 * 512
        MEGABYTE = 1024 * 1024
        GIGABYTE = MEGABYTE * 1024
    
        #Taken from original script. Purpose of this test this that expected behavior is still matched after refactoring
        try:
            with os.fdopen(os.open(self.TEST_PARAMETERS['UPLOAD_BLOCKS'], os.O_RDWR | os.O_CREAT), 'rb+') as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                gbsize = math.ceil(size / GIGABYTE)
                chunks = size // CHUNK_SIZE
                return [size, chunks, gbsize]
        except Exception as e:
            print("OS Error Message\n",e)

        
    def small_test_fanout(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps fanout {self.TEST_PARAMETERS['UPLOAD_BLOCKS']} {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/test/{self.TEST_PARAMETERS['REGIONS_FILE']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()
        output[2] = lines[2].strip()
        
        expected = self.compute_expected_output()

        self.assertEqual(output[0], f"Size of {self.TEST_PARAMETERS['UPLOAD_BLOCKS']} is {expected[0]} bytes and {expected[1]} chunks. Aligning snapshot to {expected[2]} GiB boundary.", "Script output is not expected")
        self.assertEqual(output[1], f"Spawned {len(self.CLASS_SCOPE_VARS['REGION_LIST'])} EBS Clients and started a snapshot in each region.", "Script output is not expected")

        # check snapshots available logic
        region_to_snapid_map = json.loads(output[2])
        self.CLASS_SCOPE_VARS['REGION_MAP'] = region_to_snapid_map

        for region in region_to_snapid_map:
            snap_id = region_to_snapid_map[region]

            #check that new snapshot exists
            response = None
            retry = 0
            while response is None and retry < self.TEST_PARAMETERS['MAX_RETRY']:
                try:
                    ec2 = boto3.client('ec2', region_name=region)
                    response = ec2.describe_snapshots(
                        SnapshotIds=[snap_id],
                        DryRun=False
                    )
                except Exception as e:
                    print("AWS Error Message\n", e)
                    sleep(self.TEST_PARAMETERS['RETRY_BACKOFF'])
                finally:
                    retry += 1
                
            self.assertIsNotNone(response, f"No Response! Checking Snapshot {snap_id} failed.")
            self.assertTrue((response['Snapshots'][0]['State'] == 'completed' or response['Snapshots'][0]['State'] == 'pending'), f"fanout had an error for snapshot {snap_id} in {region}.")
        
    def tearDown(self):
        super(CanaryFanoutSnapshots, self).tearDown()
        
        os.remove(f'{os.path.dirname(os.path.realpath(__file__))}/regions.txt')

        for region in self.CLASS_SCOPE_VARS['REGION_MAP']:
            snap_id = self.CLASS_SCOPE_VARS['REGION_MAP'][region]

            try:
                ec2 = boto3.client('ec2', region_name=region)
                ec2.delete_snapshot(
                SnapshotId=snap_id,
                DryRun=False
            )
            except Exception as e:
                print("AWS Error Message\n", e)
class CanaryS3Snapshot(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    CLASS_SCOPE_VARS = {}
    
    def setUp(self):
        super(CanaryS3Snapshot, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshotId'] = testing_configurations["ami-snapshot"] 
        self.TEST_PARAMETERS['DEST_S3_BUCKET'] = testing_configurations['dest-s3bucket']
        self.TEST_PARAMETERS['DEFAULT_SLEEP_TIME'] = testing_configurations['default-retry-time']
        self.TEST_PARAMETERS['MAX_RETRY_COUNT'] = testing_configurations['max-retry-count']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']

        
    def small_test_movetos3(self):
        self.CLASS_SCOPE_VARS['DESTROY'] = False #Work Around for deprovisioning test resources too early
        
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps movetos3 {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['DEST_S3_BUCKET']}"
        
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        s3 = boto3.client('s3')
        objects = []
        try:
            response = s3.list_objects_v2(Bucket=self.TEST_PARAMETERS['DEST_S3_BUCKET'], 
                Prefix=self.TEST_PARAMETERS['snapshotId'])
            objects = response['Contents']
            
            while 'NextContinuationToken' in response:
                response = s3.list_objects_v2(Bucket=self.TEST_PARAMETERS['DEST_S3_BUCKET'], 
                    Prefix=self.TEST_PARAMETERS['snapshotId'], 
                    NextContinuationToken=response['NextContinuationToken'])
                objects.extend(response['Contents'])
                
        except Exception as e:
            print("AWS Error Message\n", e)
            
        self.CLASS_SCOPE_VARS['obj_comp'] = objects
        self.assertNotEqual(len(objects), 0, f"Uploaded snapshot is not in destination ({self.TEST_PARAMETERS['DEST_S3_BUCKET']}) s3 Bucket")
        
    def small_test_getfroms3(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py --nodeps getfroms3 {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['DEST_S3_BUCKET']}"
        self.CLASS_SCOPE_VARS['DESTROY'] = True #Work Around for deprovisioning test resources too early

        print(f"\nRunning Script: {command}")
        
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        lines = result.stdout.decode('utf-8').split('\n')
        self.CLASS_SCOPE_VARS['new_snapshotId'] = lines[1].strip()
  
        #Await snapshot created successfully
        response = {'State':'pending'}
        retryCount = 0
        try:
            ec2 = boto3.client('ec2')
            while(response['State'] != 'completed') and retryCount < self.TEST_PARAMETERS['MAX_RETRY_COUNT']:
                sleep(self.TEST_PARAMETERS['DEFAULT_SLEEP_TIME'])
            
                #Request state of snapshot
                response = ec2.describe_snapshots(
                    SnapshotIds=[
                        self.CLASS_SCOPE_VARS['new_snapshotId'],
                    ],
                    DryRun=False
                )['Snapshots'][0] #Grab the only snapshot in request
                retryCount = retryCount + 1
                
        except Exception as e:
            print("AWS Error Message\n", e)
        
        self.assertEqual(response['State'], 'completed', "Fail to load from s3 bucket (May pass with higher retry count)")
        
    def tearDown(self):
        super(CanaryS3Snapshot, self).tearDown()
        
        if self.CLASS_SCOPE_VARS['DESTROY'] == True:
            if len(self.CLASS_SCOPE_VARS['obj_comp']) > 0:
                try: 
                    s3 = boto3.client('s3')
                    for component in self.CLASS_SCOPE_VARS['obj_comp']:
                        s3.delete_object(
                            Bucket=self.TEST_PARAMETERS['DEST_S3_BUCKET'],
                            Key=component['Key'])
            
                except Exception as e:
                    print("AWS Error Message (test object may not have deleted)\n", e)
                
            try:
                #Delete 'restore snapshot'
                ec2 = boto3.client('ec2')
                ec2.delete_snapshot(
                    SnapshotId=self.CLASS_SCOPE_VARS['new_snapshotId'],
                    DryRun=False
                )
            except Exception as e:
                print("AWS Error Message\n", e)

def SmallCanarySuite():
    suite = unittest.TestSuite()
    suite.addTest(CanaryListSnapshot('small_test_list'))
    suite.addTest(CanaryDownloadSnapshots('small_test_download'))
    suite.addTest(CanaryDeltadownloadSnapshots('small_test_deltadownload'))
    suite.addTest(CanaryUploadSnapshots('small_test_upload'))
    suite.addTest(CanaryCopySnapshot('small_test_copy'))
    suite.addTest(CanaryDiffSnapshots('small_test_diff'))
    suite.addTest(CanarySyncSnapshots('small_test_sync'))
    suite.addTest(CanaryMultiCloneSnapshot('small_test_multiclone'))
    suite.addTest(CanaryFanoutSnapshots('small_test_fanout'))
    suite.addTest(CanaryS3Snapshot('small_test_movetos3'))
    suite.addTest(CanaryS3Snapshot('small_test_getfroms3'))
    return suite
