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
from snapshot_factory import check_pattern

"""Method to expose test cases for each CLI command to test runner via a test suite."""
def SmallCanarySuite():
    suite = unittest.TestSuite()
    suite.addTest(CanaryListSnapshot('small_test_list'))
    suite.addTest(CanaryDownloadSnapshots('small_test_download'))
    suite.addTest(CanaryDeltadownloadSnapshots('small_test_deltadownload'))
    suite.addTest(CanaryUploadSnapshots('small_test_upload'))
    suite.addTest(CanaryCopySnapshot('small_test_copy'))
    suite.addTest(CanaryDiffSnapshots('small_test_diff'))
    # suite.addTest(CanarySyncSnapshots('small_test_sync'))
    suite.addTest(CanaryMultiCloneSnapshot('small_test_multiclone'))
    # suite.addTest(CanaryFanoutSnapshots('small_test_fanout'))
    # suite.addTest(CanaryS3Snapshot('small_test_movetos3'))
    # suite.addTest(CanaryS3Snapshot('small_test_getfroms3'))
    return suite
"""Method to retrieve configuration variables to be used by testing framework

config.yaml serves as a cache for testing resources like snapshots and s3 buckets to use in testing workflows
"""
def get_configuration():
    with open(f"{os.path.dirname(os.path.realpath(__file__))}/config.yaml", 'r') as file:
        config = yaml.safe_load(file)
    config['PATH_TO_PROJECT_DIRECTORY'] = os.path.dirname(os.path.realpath(__file__)) + '/..'
    config['PATH_TO_TEST'] = os.path.dirname(os.path.realpath(__file__)) + '/'

    return config

"""Common script output between different CLI commands to list number of chunks and bytes. 

Used to verify that the output is correct, consistent and backwards compatible with improvements.
Note: Any reporting of time to complete operation will be ignored. 
"""
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

"""Common script output between different CLI commands to list number of differential chunks and bytes. 

Used to verify that the output is correct, consistent and backwards compatible with improvements.
Note: Any reporting of time to complete operation will be ignored. 
"""
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

"""Unit tests for the "list" CLI command.
"""
class CanaryListSnapshot(unittest.TestCase):

    def setUp(self):
        super(CanaryListSnapshot, self).setUp()

        testing_configurations = get_configuration()

        self.snapshot = testing_configurations["snapshots"]["every-sector"]["snapshot-id"]
        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']


    def small_test_list(self):

        #Run script to move see size of snapshot
        command = f"python3 {self.project_directory_path}/src/main.py --nodeps list {self.snapshot}"
        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, f"src/main.py exited with FAILURE status code. Trace:\n{result}") #Ensure the script ran successfully

        #retrieve the command result
        output = result.stdout.decode('utf-8')
        output = output.split(',')[0] #Remove the time reporting

        size = compute_size_of_snapshot(self.snapshot)
        self.assertEqual(output, f"Snapshot {self.snapshot} contains {size[0]} chunks and {size[1]} bytes", "Mismatch Expected Output")

    def tearDown(self):
        super(CanaryListSnapshot, self).tearDown()


"""Unit tests for the "download" CLI command.

Additional Test Case Required:
    - Different origin region
"""
class CanaryDownloadSnapshots(unittest.TestCase):

    def setUp(self):
        super(CanaryDownloadSnapshots, self).setUp()

        testing_configurations = get_configuration()

        self.snapshot_id = testing_configurations["snapshots"]["every-sector"]["snapshot-id"]
        self.pattern = testing_configurations["snapshots"]["every-sector"]["metadata"]
        self.size = testing_configurations["snapshots"]["every-sector"]["size"]
        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']

        with open("/tmp/zeroes", "w") as outfile:
            subprocess.run(["sudo", "head", "-c" , f"{str(self.size)}G", "/dev/zero"], stdout=outfile)
        LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
        subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"], capture_output=True)
        self.raw_device = LOOP_FILE

    def small_test_download(self):
        command =f"sudo python3 {self.project_directory_path}/src/main.py --nodeps download {self.snapshot_id} {self.raw_device}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully

        output = ['','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()

        output[0] = output[0].split(',')[0] #Remove the time reporting

        expected = compute_size_of_snapshot(self.snapshot_id)

        self.assertEqual(output[0], f"Snapshot {self.snapshot_id} contains {expected[0]} chunks and {expected[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], f"['{self.raw_device}']")

        patterns = []
        patterns.append(self.pattern)
        self.assertTrue(check_pattern(self.snapshot_id, self.size, patterns, self.raw_device), "Snapshot data is not correct")


    def tearDown(self):
        super(CanaryDownloadSnapshots, self).tearDown()

        subprocess.run(["sudo", "losetup", "-d", self.raw_device])


"""Unit tests for the "deltadownload" CLI command.
"""
class CanaryDeltadownloadSnapshots(unittest.TestCase):

    def setUp(self):
        super(CanaryDeltadownloadSnapshots, self).setUp()

        testing_configurations = get_configuration()

        # snapshot one is parent of snapshot two
        self.snapshot_id_one = testing_configurations["snapshots"]["every-second-sector"]["snapshot-id"]
        self.snapshot_id_two = testing_configurations["snapshots"]["every-fourth-sector"]["snapshot-id"]
        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.size = testing_configurations["snapshots"]["every-second-sector"]["size"]
        
        with open("/tmp/zeroes", "w") as outfile:
            subprocess.run(["sudo", "head", "-c" , f"{str(self.size)}G", "/dev/zero"], stdout=outfile)
        LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
        subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"], capture_output=True)
        self.raw_device = LOOP_FILE

    def small_test_deltadownload(self):
        command =f"sudo python3 {self.project_directory_path}/src/main.py --nodeps deltadownload {self.snapshot_id_one} {self.snapshot_id_two} {self.raw_device}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully

        output = ['','','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()

        output[0] = output[0].split(',')[0] #Remove the time reporting

        expected = compute_size_of_diff(self.snapshot_id_one, self.snapshot_id_two)

        self.assertEqual(output[0], f"Changes between {self.snapshot_id_one} and {self.snapshot_id_two} contain {expected[0]} chunks and {expected[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], f"['{self.raw_device}']")

    def tearDown(self):
        super(CanaryDeltadownloadSnapshots, self).tearDown()

        subprocess.run(["sudo", "losetup", "-d", self.raw_device])

"""Unit tests for the "upload" CLI command.

Additional Tests Required:
    - upload snapshot that has a parent snapshot
"""
class CanaryUploadSnapshots(unittest.TestCase):

    def setUp(self):
        super(CanaryUploadSnapshots, self).setUp()

        testing_configurations = get_configuration()

        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        
        with open("/tmp/zeroes", "w") as outfile:
            subprocess.run(["sudo", "head", "-c" , "1G", "/dev/urandom"], stdout=outfile)
        LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
        subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"], capture_output=True)
        self.raw_device = LOOP_FILE

    def compute_expected_output(self):
        CHUNK_SIZE = 1024 * 512
        MEGABYTE = 1024 * 1024
        GIGABYTE = MEGABYTE * 1024

        #Taken from original script. Purpose of this test this that expected behavior is still matched after refactoring
        try:
            with os.fdopen(os.open(self.raw_device, os.O_RDWR | os.O_CREAT), 'rb+') as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                gbsize = math.ceil(size / GIGABYTE)
                chunks = size // CHUNK_SIZE
                return [size, chunks]
        except Exception as e:
            print("OS Error Message\n",e)


    def small_test_upload(self):
        command =f"python3 {self.project_directory_path}/src/main.py --nodeps upload {self.raw_device}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully

        output = ['','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[2].strip()
        self.new_snapshot_id = lines[4].strip()

        expected = self.compute_expected_output()

        self.assertEqual(output[0], f"Size of {self.raw_device} is {expected[0]} bytes and {expected[1]} chunks", "Script output is not expected\nNote: Test will fail is FULL_COPY = False. (By reducing size of snapshot expected output is too large)")
        self.assertEqual(output[1], f"Total chunks uploaded {expected[1]}", "Script output is not expected")

    def tearDown(self):
        super(CanaryUploadSnapshots, self).tearDown()

        try:
            ec2 = boto3.client('ec2')
            #Delete 'restore snapshot'
            ec2.delete_snapshot(
                SnapshotId=self.new_snapshot_id,
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)

        subprocess.run(["sudo", "losetup", "-d", self.raw_device])

"""Unit tests for the "copy" CLI command.

Additional Tests Required:
    - cross-region copy
"""
class CanaryCopySnapshot(unittest.TestCase):

    def setUp(self):
        super(CanaryCopySnapshot, self).setUp()

        testing_configurations = get_configuration()

        self.snapshot_id = testing_configurations["snapshots"]["every-sector"]["snapshot-id"]
        self.pattern = testing_configurations["snapshots"]["every-sector"]["metadata"]
        self.size = testing_configurations["snapshots"]["every-sector"]["size"]
        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']


    def small_test_copy(self):
        command =f"python3 {self.project_directory_path}/src/main.py --nodeps copy {self.snapshot_id}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully

        output = ''
        lines = result.stdout.decode('utf-8').split('\n')
        output = lines[0].strip()
        self.new_snapshot_id = lines[2].strip()

        output = output.split(',')[0] #Remove the time reporting

        size = compute_size_of_snapshot(self.snapshot_id)

        self.assertEqual(output, f"Snapshot {self.snapshot_id} contains {size[0]} chunks and {size[1]} bytes", "Script output is not expected")

        #check that new snapshot exists
        response = None
        try:
            ec2 = boto3.client('ec2')
            response = ec2.describe_snapshots(
                SnapshotIds=[self.new_snapshot_id],
                DryRun=False
            )


        except Exception as e:
            print("AWS Error Message\n", e)

        self.assertIsNotNone(response, "No Response!")
        self.assertTrue((response['Snapshots'][0]['State'] == 'completed' or response['Snapshots'][0]['State'] == 'pending'), "Copy had an error!")

        patterns = []
        patterns.append(self.pattern)
        self.assertTrue(check_pattern(self.new_snapshot_id, self.size, patterns), "Copy data is inconsistent with original snapshot data")


    def tearDown(self):
        super(CanaryCopySnapshot, self).tearDown()

        try:
            ec2 = boto3.client('ec2')
            #Delete 'restore snapshot'
            ec2.delete_snapshot(
                SnapshotId=self.new_snapshot_id,
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)

"""Unit tests for the "diff" CLI command.
"""
class CanaryDiffSnapshots(unittest.TestCase):

    def setUp(self):
        super(CanaryDiffSnapshots, self).setUp()

        testing_configurations = get_configuration()

        self.snapshot_id_one = testing_configurations["snapshots"]["every-second-sector"]["snapshot-id"]
        self.snapshot_id_two = testing_configurations["snapshots"]["every-fourth-sector"]["snapshot-id"]
        self.path_to_project_directory = testing_configurations['PATH_TO_PROJECT_DIRECTORY']

    def small_test_diff(self):
        command =f"python3 {self.path_to_project_directory}/src/main.py --nodeps diff {self.snapshot_id_one} {self.snapshot_id_two}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, f"src/main.py exited with FAILURE status code. trace:\n{result}") #Ensure the script ran successfully

        output = ''
        lines = result.stdout.decode('utf-8').split('\n')
        output = lines[0].strip()

        output = output.split(',')[0] #Remove the time reporting

        size = compute_size_of_diff(self.snapshot_id_one, self.snapshot_id_two)

        self.assertEqual(output, f"Changes between {self.snapshot_id_one} and {self.snapshot_id_two} contain {size[0]} chunks and {size[1]} bytes", "Script output is not expected")


    def tearDown(self):
        super(CanaryDiffSnapshots, self).tearDown()

"""Unit tests for the "sync" CLI command.

Additional Tests Required:
    - cross-region sync
    - full copy test
"""
class CanarySyncSnapshots(unittest.TestCase):

    def setUp(self):
        super(CanarySyncSnapshots, self).setUp()

        testing_configurations = get_configuration()
        self.MAX_BACKOFF = testing_configurations["max-backoff-retry"]
        self.BACKOFF_TIME = testing_configurations["backoff-time-seconds"]

        self.snapshot_id_one = testing_configurations["snapshots"]["every-fourth-sector"]["snapshot-id"]
        self.snapshot_id_two = testing_configurations["snapshots"]["every-third-and-fourth-sector"]["snapshot-id"]
        self.pattern_one = testing_configurations["snapshots"]["every-fourth-sector"]["metadata"]
        self.pattern_two = testing_configurations["snapshots"]["every-third-and-fourth-sector"]["metadata"]
        self.size = testing_configurations["snapshots"]["every-fourth-sector"]["size"]
        self.snapshot_two_parent = testing_configurations["snapshots"]["every-third-and-fourth-sector"]["parent"]
        self.path_to_project_directory = testing_configurations['PATH_TO_PROJECT_DIRECTORY']

        result = subprocess.run(["python3", f"{self.path_to_project_directory}/src/main.py", "copy", self.snapshot_id_one], capture_output=True)
        lines = result.stdout.decode('utf-8').split('\n')
        self.copy_to_sync_snapshot_id = lines[2].strip()

         #check that new snapshot exists from perspective of ebs direct api
        retry = 0
        while True and retry < 30:
            if subprocess.run(["python3", f"{self.path_to_project_directory}/src/main.py", "--nodeps", "list", self.copy_to_sync_snapshot_id], capture_output=True).returncode == 0:
                break
            retry += 1
            sleep(self.BACKOFF_TIME)


    def small_test_sync(self):
        command =f"python3 {self.path_to_project_directory}/src/main.py --nodeps sync {self.snapshot_id_one} {self.snapshot_id_two} {self.copy_to_sync_snapshot_id}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, f"src/main.py exited with FAILURE status code\n{result}") #Ensure the script ran successfully

        output = ''
        lines = result.stdout.decode('utf-8').split('\n')
        output = lines[0].strip()
        self.new_snapshot_id = lines[1].strip()

        output = output.split(',')[0] #Remove the time reporting

        size = compute_size_of_diff(self.snapshot_id_one, self.snapshot_id_two)

        self.assertEqual(output, f"Changes between {self.snapshot_id_one} and {self.snapshot_id_two} contain {size[0]} chunks and {size[1]} bytes", "Script output is not expected")

        #check that new snapshot exists
        response = None
        retry = 0
        while response is None and retry < self.MAX_BACKOFF:
            try:
                ec2 = boto3.client('ec2')
                response = ec2.describe_snapshots(
                    SnapshotIds=[self.new_snapshot_id],
                    DryRun=False
                )
            except Exception as e:
                print("AWS Error Message\n", e)
                sleep(self.BACKOFF_TIME)
            finally:
                retry += 1

        self.assertIsNotNone(response, f"No Response! Checking Snapshot {self.new_snapshot_id} failed.")
        self.assertTrue((response['Snapshots'][0]['State'] == 'completed' or response['Snapshots'][0]['State'] == 'pending'), "Sync had an error!")

        patterns = []
        patterns.append(self.pattern_one)
        patterns.append(self.pattern_two)
        self.assertTrue(check_pattern(self.new_snapshot_id, self.size, patterns), "Snapshot data is inconsistent")


    def tearDown(self):
        super(CanarySyncSnapshots, self).tearDown()

        try:
            ec2 = boto3.client('ec2')
            #Delete 'restore snapshot'
            ec2.delete_snapshot(
                SnapshotId=self.copy_to_sync_snapshot_id,
                DryRun=False
            )
            ec2.delete_snapshot(
                SnapshotId=self.new_snapshot_id,
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)

"""Unit tests for the "multiclone" CLI command.
"""
class CanaryMultiCloneSnapshot(unittest.TestCase):

    def setUp(self):
        super(CanaryMultiCloneSnapshot, self).setUp()

        testing_configurations = get_configuration()

        self.snapshot_id = testing_configurations["snapshots"]["every-sector"]["snapshot-id"]
        self.pattern = testing_configurations["snapshots"]["every-sector"]["metadata"]
        self.size = testing_configurations["snapshots"]["every-sector"]["size"]
        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']

        self.loop_devices = []
        for i in range(5):
            with open("/tmp/zeroes", "w") as outfile:
                subprocess.run(["sudo", "head", "-c" , f"{self.size}G", "/dev/zero"], stdout=outfile)
            LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
            subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"])
            self.loop_devices.append(LOOP_FILE)

        self.path_to_temp_file = self.project_directory_path + "/test/test.txt"

        with open(self.path_to_temp_file, mode='a') as f:
            for LOOP_DEVICE in self.loop_devices:
                f.writelines(LOOP_DEVICE+"\n")

    def small_test_multiclone(self):
        command =f"sudo python3 {self.project_directory_path}/src/main.py --nodeps multiclone {self.snapshot_id} {self.path_to_temp_file}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, f"src/main.py exited with FAILURE status code\n{result}") #Ensure the script ran successfully

        output = ['','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()

        output[0] = output[0].split(',')[0] #Remove the time reporting

        size = compute_size_of_snapshot(self.snapshot_id)

        self.assertEqual(output[0], f"Snapshot {self.snapshot_id} contains {size[0]} chunks and {size[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], str(self.loop_devices))

        patterns = []
        patterns.append(self.pattern)
        for LOOP_FILE in self.loop_devices:
            self.assertTrue(check_pattern(self.snapshot_id, self.size, patterns, LOOP_FILE), f"Data on loop device {LOOP_FILE} does not match {self.snapshot_id}")

    def tearDown(self):
        super(CanaryMultiCloneSnapshot, self).tearDown()

        os.remove(self.path_to_temp_file)

        for LOOP_FILE in self.loop_devices:
            subprocess.run(["sudo", "losetup", "-d", LOOP_FILE])

"""Unit tests for the "fanout" CLI command.
"""
class CanaryFanoutSnapshots(unittest.TestCase):

    def setUp(self):
        super(CanaryFanoutSnapshots, self).setUp()

        testing_configurations = get_configuration()

        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.snapshot_id = testing_configurations["snapshots"]["every-sector"]["snapshot-id"]
        self.size = testing_configurations["snapshots"]["every-sector"]["size"]
        self.pattern = testing_configurations["snapshots"]["every-sector"]["metadata"]
        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']

        self.max_retry = testing_configurations['max-backoff-retry']
        self.backoff_time = testing_configurations['backoff-time-seconds']
        self.regions_file = f"{self.project_directory_path}/test/regions.txt"
        self.regions = ['us-east-1', 'us-east-2','us-west-1', 'us-west-2']
        with open(self.regions_file, 'w') as f:
            for r in self.regions:
                f.write(r + '\n')

        with open("/tmp/zeroes", "w") as outfile:
            subprocess.run(["sudo", "head", "-c" , f"{self.size}G", "/dev/zero"], stdout=outfile)
        LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
        subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"])

        subprocess.run(["sudo", "python3", f"{self.project_directory_path}/src/main.py", "--nodeps", "download", self.snapshot_id, LOOP_FILE])
        self.upload_location = LOOP_FILE

    def compute_expected_output(self):
        CHUNK_SIZE = 1024 * 512
        MEGABYTE = 1024 * 1024
        GIGABYTE = MEGABYTE * 1024

        #Taken from original script. Purpose of this test this that expected behavior is still matched after refactoring
        try:
            with os.fdopen(os.open(self.upload_location, os.O_RDWR | os.O_CREAT), 'rb+') as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                gbsize = math.ceil(size / GIGABYTE)
                chunks = size // CHUNK_SIZE
                return [size, chunks, gbsize]
        except Exception as e:
            print("OS Error Message\n",e)


    def small_test_fanout(self):
        command =f"sudo python3 {self.project_directory_path}/src/main.py --nodeps fanout {self.upload_location} {self.regions_file}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, f"src/main.py exited with FAILURE status code\n{result}") #Ensure the script ran successfully

        output = ['','','']
        lines = result.stdout.decode('utf-8').split('\n')
        output[0] = lines[0].strip()
        output[1] = lines[1].strip()
        output[2] = lines[2].strip()

        expected = self.compute_expected_output()

        self.assertEqual(output[0], f"Size of {self.upload_location} is {expected[0]} bytes and {expected[1]} chunks. Aligning snapshot to {expected[2]} GiB boundary.", "Script output is not expected")
        self.assertEqual(output[1], f"Spawned {len(self.regions)} EBS Clients and started a snapshot in each region.", "Script output is not expected")

        # check snapshots available logic
        self.region_map = json.loads(output[2])

        counter = 0
        for region in self.region_map:
            snap_id = self.region_map[region]

            #check that new snapshot exists
            response = None
            retry = 0
            while response is None and retry < self.max_retry:
                try:
                    ec2 = boto3.client('ec2', region_name=region)
                    response = ec2.describe_snapshots(
                        SnapshotIds=[snap_id],
                        DryRun=False
                    )
                except Exception as e:
                    print("AWS Error Message\n", e)
                    sleep(self.backoff_time)
                finally:
                    retry += 1

            self.assertIsNotNone(response, f"No Response! Checking Snapshot {snap_id} failed.")
            self.assertTrue((response['Snapshots'][0]['State'] == 'completed' or response['Snapshots'][0]['State'] == 'pending'), f"fanout had an error for snapshot {snap_id} in {region}.")

            # Validate data
            with open("/tmp/zeroes", "w") as outfile:
                subprocess.run(["sudo", "head", "-c" , f"{self.size}G", "/dev/zero"], stdout=outfile)
            LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
            subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"])

            subprocess.run(["sudo", "-o", region, "download", snap_id, LOOP_FILE], capture_output=True)
            patterns = []
            patterns.append(self.pattern)
            self.assertTrue(check_pattern(self.snapshot_id, self.size, patterns, LOOP_FILE), f"Data on loop device {LOOP_FILE} does not match {self.snapshot_id}")
            counter +=1
            print(counter)


    def tearDown(self):
        super(CanaryFanoutSnapshots, self).tearDown()

        os.remove(self.regions_file)

        for region in self.region_map:
            snap_id = self.region_map[region]

            try:
                ec2 = boto3.client('ec2', region_name=region)
                ec2.delete_snapshot(
                    SnapshotId=snap_id,
                    DryRun=False
                )
            except Exception as e:
                print("AWS Error Message\n", e)
        subprocess.run(["sudo", "losetup", "-d", self.upload_location])
"""Unit tests for the "movetos3" and "getfroms3 CLI commands.

Note that these tests are a little different as they run movetos3 to produce data
    then run getfroms3 to consume and validate the data. After both tests have concluded then
    created test data will be deprovisioned, not beforehand!

Additional Tests Required:
    - Cross account transfer
"""
class CanaryS3Snapshot(unittest.TestCase):

    def setUp(self):
        super(CanaryS3Snapshot, self).setUp()

        testing_configurations = get_configuration()

        self.project_directory_path = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.MAX_BACKOFF = testing_configurations["max-backoff-retry"]
        self.BACKOFF_TIME = testing_configurations["backoff-time-seconds"]
        self.snapshot_id = testing_configurations["snapshots"]["every-sector"]["snapshot-id"]
        self.size = testing_configurations["snapshots"]["every-sector"]["size"]
        self.pattern = testing_configurations["snapshots"]["every-sector"]["metadata"]
        self.s3_bucket = testing_configurations['s3-bucket']

        self.destroy = False


    def small_test_movetos3(self):
        self.destroy = False #Work Around for deprovisioning test resources too early

        command =f"python3 {self.project_directory_path}/src/main.py --nodeps movetos3 {self.snapshot_id} {self.s3_bucket}"

        print(f"\nRunning Script: {command}")
        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully

        s3 = boto3.client('s3')
        objects = []
        try:
            response = s3.list_objects_v2(Bucket=self.s3_bucket,
                Prefix=self.snapshot_id)
            objects = response['Contents']

            while 'NextContinuationToken' in response:
                response = s3.list_objects_v2(Bucket=self.s3_bucket,
                    Prefix=self.snapshot_id,
                    NextContinuationToken=response['NextContinuationToken'])
                objects.extend(response['Contents'])

        except Exception as e:
            print("AWS Error Message\n", e)

        self.obj_comp = objects
        self.assertNotEqual(len(objects), 0, f"Uploaded snapshot is not in destination ({self.s3_bucket}) s3 Bucket")

    def small_test_getfroms3(self):
        command =f"python3 {self.project_directory_path}/src/main.py --nodeps getfroms3 {self.snapshot_id} {self.s3_bucket}"
        self.destroy = True #Work Around for deprovisioning test resources too early

        print(f"\nRunning Script: {command}")

        args = command.split(' ')
        result = subprocess.run(args, capture_output=True)
        self.assertEqual(result.returncode, 0, f"src/main.py exited with FAILURE status code\n{result}") #Ensure the script ran successfully

        lines = result.stdout.decode('utf-8').split('\n')
        self.new_snapshot_id = lines[1].strip()

        #Await snapshot created successfully
        response = {'State':'pending'}
        retryCount = 0
        try:
            ec2 = boto3.client('ec2')
            while(response['State'] != 'completed') and retryCount < self.MAX_BACKOFF:
                sleep(self.BACKOFF_TIME)

                #Request state of snapshot
                response = ec2.describe_snapshots(
                    SnapshotIds=[
                        self.new_snapshot_id,
                    ],
                    DryRun=False
                )['Snapshots'][0] #Grab the only snapshot in request
                retryCount = retryCount + 1

        except Exception as e:
            print("AWS Error Message\n", e)

        self.assertEqual(response['State'], 'completed', "Fail to load from s3 bucket (May pass with higher retry count)")

        # check that snapshot data is correct
        patterns = []
        patterns.append(self.pattern)
        self.assertTrue(check_pattern(self.new_snapshot_id, self.size, patterns), "Snapshot data is not correct")

    def tearDown(self):
        super(CanaryS3Snapshot, self).tearDown()

        if self.destroy == True:
            if len(self.obj_comp) > 0:
                try:
                    s3 = boto3.client('s3')
                    for component in self.obj_comp:
                        s3.delete_object(
                            Bucket=self.s3_bucket,
                            Key=component['Key'])

                except Exception as e:
                    print("AWS Error Message (test object may not have deleted)\n", e)

            try:
                #Delete 'restore snapshot'
                ec2 = boto3.client('ec2')
                ec2.delete_snapshot(
                    SnapshotId=self.new_snapshot_id,
                    DryRun=False
                )
            except Exception as e:
                print("AWS Error Message\n", e)
