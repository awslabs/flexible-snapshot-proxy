import json
from mailbox import linesep
import unittest
import os
import boto3
import yaml
import math
from time import sleep
from shutil import rmtree

PATH_TO_FULL_STACK_TESTING = '/test/full_stack' #defined by git directory structure

def read_configuration_file():
    with open(f"{os.path.dirname(os.path.realpath(__file__))}/config_personal.yaml", 'r') as file:
        config = yaml.safe_load(file)
    config['PATH_TO_PROJECT_DIRECTORY'] = os.path.dirname(os.path.realpath(__file__)) + '/../..'
    
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

class WorkflowMoveToS3RestoreFromS3(unittest.TestCase):
    TEST_PARAMETERS = {}
    CLASS_SCOPE_VARS = {}
    
    def setUp(self):
        super(WorkflowMoveToS3RestoreFromS3, self).setUp()
        #Read parameters from YAML
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['ORG_SNAP'] = testing_configurations['ami-snapshot']
        self.TEST_PARAMETERS['DEST_S3_BUCKET'] = testing_configurations['dest-s3bucket']
        self.TEST_PARAMETERS['EC2_INSTANCE_ID'] = testing_configurations['ec2-instanceId']
        self.TEST_PARAMETERS['ORIGINAL_BOOT_DRIVE_PATH'] = testing_configurations['original-ami-path']
        self.TEST_PARAMETERS['BACKUP_BOOT_DRIVE_PATH'] = testing_configurations['restored-ami-path']
        self.TEST_PARAMETERS['DEFAULT_SLEEP_TIME'] = testing_configurations['default-retry-time']
        self.TEST_PARAMETERS['AWS_ORIGIN_REGION'] = testing_configurations['aws-origin-region']
        self.TEST_PARAMETERS['AWS_ORIGIN_AZ'] = testing_configurations['aws-availability-zone']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
    
    def test_move_to_s3_and_get_from_s3(self):
        ec2 = boto3.client('ec2')
        
        #Run script to move snapshot to s3. Then move from s3 to new snapshot
        command_one = f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py movetos3 {self.TEST_PARAMETERS['ORG_SNAP']}"
        command_two = f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py getfroms3 {self.TEST_PARAMETERS['ORG_SNAP']} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"
        print(f"Running Script: {command_one}")
        self.assertEqual(os.system(f"{command_one}"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
        print(f"Running Script: {command_one}")
        self.assertEqual(os.system(f"{command_two}"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
        
        new_snapshotId = ''
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            new_snapshotId = lines[-1].strip()
        self.CLASS_SCOPE_VARS['new_snapshotId'] = new_snapshotId #For tearDown (deallocation of AWS resource)
        
        response = {'State':'pending'}
        while(response['State'] != 'completed'):
            print("Waiting for snapshot creation to complete...")
            sleep(self.TEST_PARAMETERS['DEFAULT_SLEEP_TIME'])
            
            response = ec2.describe_snapshots(
                SnapshotIds=[
                    new_snapshotId,
                ],
                DryRun=False
            )['Snapshots'][0] #Grab the only snapshot in request
        
        print(f"SNAPSHOT {new_snapshotId} CREATION COMPLETE\n")

        
        #Create new EBS volume from getroms3 restore
        new_volume = ec2.create_volume(
            Size=10,
            Iops=3000,
            VolumeType='gp3',
            Encrypted=False,
            AvailabilityZone=f"{self.TEST_PARAMETERS['AWS_ORIGIN_REGION']}{self.TEST_PARAMETERS['AWS_ORIGIN_AZ']}",
            SnapshotId=new_snapshotId,
            TagSpecifications=[
                {
                    'ResourceType': 'volume',
                    'Tags': [
                        {
                            'Key': 'ebsPlaygroundFullStackTest-testVolume',
                            'Value': 'test-move-to-s3-and-get-from-s3'
                        }
                    ]
                }
            ]
        )
        
        self.CLASS_SCOPE_VARS['new_volumeId'] = new_volume['VolumeId'] #For tearDown (deallocation of AWS resource)
        
        response = {'State':'creating'}
        while(response['State'] != 'available'):
            print("Waiting for new volume to be available...")
            sleep(self.TEST_PARAMETERS['DEFAULT_SLEEP_TIME'])
            
            response = ec2.describe_volumes(
                VolumeIds=[
                    new_volume['VolumeId'],
                ],
                DryRun=False
            )['Volumes'][0]
        
        print(f"EBS VOLUME {new_volume['VolumeId']} IS AVAILABLE\n")
        
        
        ec2.attach_volume(
            Device=self.TEST_PARAMETERS['BACKUP_BOOT_DRIVE_PATH'],
            InstanceId=self.TEST_PARAMETERS['EC2_INSTANCE_ID'],
            VolumeId=new_volume['VolumeId'],
            DryRun=False
        )
        
        response = {'State':'available'}
        while(response['State'] != 'in-use'):
            print("Waiting for volume to be attach...")
            sleep(self.TEST_PARAMETERS['DEFAULT_SLEEP_TIME'])
            
            response = ec2.describe_volumes(
                VolumeIds=[
                    new_volume['VolumeId'],
                ],
                DryRun=False
            )['Volumes'][0]
        
        print(f"EBS VOLUME {new_volume['VolumeId']} IS ATTACHED TO EC2 INSTANCE {self.TEST_PARAMETERS['EC2_INSTANCE_ID']} as {self.TEST_PARAMETERS['BACKUP_BOOT_DRIVE_PATH']}\n")
        
        #Use md5sum to create a checksum for the original and 'restored' drives
        print("Preforming test: Generating md5sum checksums for original and restored ebs volumes and comparing (this may take a while)...")
        os.system(f"md5sum {self.TEST_PARAMETERS['ORIGINAL_BOOT_DRIVE_PATH']} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp1.txt")
        os.system(f"md5sum {self.TEST_PARAMETERS['BACKUP_BOOT_DRIVE_PATH']} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp2.txt")
        
        og_checksum = ""
        new_checksum = ""
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp1.txt") as f:
            lines = f.readlines()
            og_checksum = lines[0].strip()
        
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp2.txt") as f:
            lines = f.readlines()
            new_checksum = lines[0].strip()
        
        self.assertEquals(og_checksum, new_checksum, "Checksums on the volumes do not match!")
        
        
        
    def tearDown(self):
        ec2 = boto3.client('ec2')
        super(WorkflowMoveToS3RestoreFromS3, self).tearDown()
        #Remove Temp Files and AWS resources if they exist
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
            
        try:
            #Delete 'restore snapshot'
            ec2.delete_snapshot(
                SnapshotId=self.CLASS_SCOPE_VARS['new_snapshotId'],
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)
        
        try:
            #Request state of volume
            response = ec2.describe_volumes(
                VolumeIds=[
                    self.CLASS_SCOPE_VARS['new_volumeId'],
                ],
                DryRun=False
            )['Volumes'][0]
            
            if response['State'] == 'in-use': #Do not detach an instance not in use!
                ec2.detach_volume(
                    Device=self.TEST_PARAMETERS['BACKUP_BOOT_DRIVE_PATH'],
                    Force=True,
                    InstanceId=self.TEST_PARAMETERS['EC2_INSTANCE_ID'],
                    VolumeId=self.CLASS_SCOPE_VARS['new_volumeId'],
                    DryRun=False
                )
            #Wait for drive to detach
            while response['State'] == 'in-use':
                sleep(self.TEST_PARAMETERS['DEFAULT_SLEEP_TIME'])
                response = ec2.describe_volumes(
                    VolumeIds=[
                        self.CLASS_SCOPE_VARS['new_volumeId'],
                    ],
                    DryRun=False
                )['Volumes'][0]
        
            ec2.delete_volume(
                VolumeId=self.CLASS_SCOPE_VARS['new_volumeId'],
                DryRun=False
            )
        except Exception as e:
            print("AWS Error Message\n", e)
            
class CanaryListSnapshot(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryListSnapshot, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['ORG_SNAP'] = testing_configurations['ami-snapshot']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        
    
    def small_test_list(self):
        
        #Run script to move see size of snapshot
        command = f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py list {self.TEST_PARAMETERS['ORG_SNAP']}"
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
        
        #retrieve the command result
        output = ''
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output = lines[0].strip()
            
        output = output.split(',')[0] #Remove the time reporting
        
        size = compute_size_of_snapshot(self.TEST_PARAMETERS['ORG_SNAP'])    
        self.assertEqual(output, f"Snapshot {self.TEST_PARAMETERS['ORG_SNAP']} contains {size[0]} chunks and {size[1]} bytes", "Mismatch Expected Output")
        
    def tearDown(self):
        super(CanaryListSnapshot, self).tearDown()
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
            
    
class CanaryDownloadSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryDownloadSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshotId'] = testing_configurations['small-volume-snapshots']['full']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE'] = testing_configurations['small-volume-path']
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        
    def small_test_download(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py download {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','']
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output[0] = lines[0].strip()
            output[1] = lines[1].strip()
            
        output[0] = output[0].split(',')[0] #Remove the time reporting
        
        expected = compute_size_of_snapshot(self.TEST_PARAMETERS["snapshotId"])

        self.assertEqual(output[0], f"Snapshot {self.TEST_PARAMETERS['snapshotId']} contains {expected[0]} chunks and {expected[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], f"['{self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}']")
        
    def tearDown(self):
        super(CanaryDownloadSnapshots, self).tearDown()
        
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))

class CanaryDeltadownloadSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    
    def setUp(self):
        super(CanaryDeltadownloadSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['snapshot1'] = testing_configurations['small-volume-snapshots']['full']
        self.TEST_PARAMETERS['snapshot2'] = testing_configurations['small-volume-snapshots']['half']
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE'] = testing_configurations['small-volume-path']
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        
    def small_test_deltadownload(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py deltadownload {self.TEST_PARAMETERS['snapshot1']} {self.TEST_PARAMETERS['snapshot2']} {self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','','']
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output[0] = lines[0].strip()
            output[1] = lines[1].strip()
            
        output[0] = output[0].split(',')[0] #Remove the time reporting
        
        expected = compute_size_of_diff(self.TEST_PARAMETERS['snapshot1'], self.TEST_PARAMETERS['snapshot2'])

        self.assertEqual(output[0], f"Changes between {self.TEST_PARAMETERS['snapshot1']} and {self.TEST_PARAMETERS['snapshot2']} contain {expected[0]} chunks and {expected[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], f"['{self.TEST_PARAMETERS['PATH_TO_RAW_DEVICE']}']")
        
    def tearDown(self):
        super(CanaryDeltadownloadSnapshots, self).tearDown()
        
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
            
class CanaryUploadSnapshots(unittest.TestCase):
    
    TEST_PARAMETERS = {}
    CLASS_SCOPE_VARS = {}
    
    def setUp(self):
        super(CanaryUploadSnapshots, self).setUp()
        
        testing_configurations = read_configuration_file()
        
        self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY']
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        self.TEST_PARAMETERS['UPLOAD_BLOCKS'] = testing_configurations['small-volume-path']
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        
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
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py upload {self.TEST_PARAMETERS['UPLOAD_BLOCKS']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','']
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output[0] = lines[0].strip()
            output[1] = lines[2].strip()
            self.CLASS_SCOPE_VARS['new_snapshotId'] = lines[4].strip()
        
        expected = self.compute_expected_output()

        self.assertEqual(output[0], f"Size of file is {expected[0]} bytes and {expected[1]} chunks", "Script output is not expected\nNote: Test will fail is FULL_COPY = False. (By reducing size of snapshot expected output is too large)")
        self.assertEqual(output[1], f"Total chunks uploaded {expected[1]}", "Script output is not expected")
        
    def tearDown(self):
        super(CanaryUploadSnapshots, self).tearDown()
        
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
        
        
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
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])

        
    def small_test_copy(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py copy {self.TEST_PARAMETERS['snapshotId']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ''
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output = lines[0].strip()
            self.CLASS_SCOPE_VARS['new_snapshotId'] = lines[-1].strip()
            
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
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
        
        
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
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        
    def small_test_diff(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py diff {self.TEST_PARAMETERS['snapshotId_1']} {self.TEST_PARAMETERS['snapshotId_2']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ''
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output = lines[0].strip()
            
        output = output.split(',')[0] #Remove the time reporting
        
        size = compute_size_of_diff(self.TEST_PARAMETERS['snapshotId_1'], self.TEST_PARAMETERS['snapshotId_2'])

        self.assertEqual(output, f"Changes between {self.TEST_PARAMETERS['snapshotId_1']} and {self.TEST_PARAMETERS['snapshotId_2']} contain {size[0]} chunks and {size[1]} bytes", "Script output is not expected")
        
        
    def tearDown(self):
        super(CanaryDiffSnapshots, self).tearDown()
        
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
            
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
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])

        
    def small_test_sync(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py sync {self.TEST_PARAMETERS['snapshotId_1']} {self.TEST_PARAMETERS['snapshotId_2']} {self.TEST_PARAMETERS['snapshotId_parent']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ''
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
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
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
        
        
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
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/test.txt", mode='a').close() #Create the file to be written to
        
    def small_test_multiclone(self):
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py multiclone {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/test.txt"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','']
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output[0] = lines[0].strip()
            output[1] = lines[1].strip()
            
        output[0] = output[0].split(',')[0] #Remove the time reporting
        
        size = compute_size_of_snapshot(self.TEST_PARAMETERS["snapshotId"])

        self.assertEqual(output[0], f"Snapshot {self.TEST_PARAMETERS['snapshotId']} contains {size[0]} chunks and {size[1]} bytes", "Script output is not expected")
        self.assertEqual(output[1], "[]") #todo will be fixed once script is functional
        
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
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        self.TEST_PARAMETERS['UPLOAD_BLOCKS'] = testing_configurations['small-volume-path']
        self.TEST_PARAMETERS['MAX_RETRY'] = testing_configurations['max-retry-count']
        self.TEST_PARAMETERS['RETRY_BACKOFF'] = testing_configurations['default-retry-time']
        self.TEST_PARAMETERS['REGIONS_FILE'] = "regions.txt"
        regions = []
        lines = open(f"{self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/test/full_stack/{self.TEST_PARAMETERS['REGIONS_FILE']}", 'r')
        for line in lines:
            regions.append(line.strip())
        self.CLASS_SCOPE_VARS['REGION_LIST'] = regions
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        
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
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py fanout {self.TEST_PARAMETERS['UPLOAD_BLOCKS']} {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/test/full_stack/{self.TEST_PARAMETERS['REGIONS_FILE']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        output = ['','','']
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            output[0] = lines[0].strip()
            output[1] = lines[1].strip()
            output[2] = lines[2].strip()
        
        expected = self.compute_expected_output()

        self.assertEqual(output[0], f"Size of file is {expected[0]} bytes and {expected[1]} chunks. Aligning snapshot to {expected[2]} GiB boundary.", "Script output is not expected")
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
        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
        
        
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
        self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'] = testing_configurations['PATH_TO_PROJECT_DIRECTORY'] + PATH_TO_FULL_STACK_TESTING + '/temp'
        
        os.mkdir(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])

        
    def small_test_movetos3(self):
        self.CLASS_SCOPE_VARS['DESTROY'] = False #Work Around for deprovisioning test resources too early
        
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py movetos3 {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['DEST_S3_BUCKET']}"
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(command), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
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
        command =f"python3 {self.TEST_PARAMETERS['PATH_TO_PROJECT_DIRECTORY']}/src/main.py getfroms3 {self.TEST_PARAMETERS['snapshotId']} {self.TEST_PARAMETERS['DEST_S3_BUCKET']}"
        
        self.CLASS_SCOPE_VARS['DESTROY'] = True #Work Around for deprovisioning test resources too early
        
        print(f"Running Script: {command}")
        self.assertEqual(os.system(f"{command} > {self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt"), 0, "src/main.py exited with FAILURE status code") #Ensure the script ran successfully
  
        with open(f"{self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY']}/temp.txt") as f:
            lines = f.readlines()
            self.CLASS_SCOPE_VARS['new_snapshotId'] = lines[-1].strip()
  
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

        try:
            rmtree(self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'])
        except OSError as e:
            print("Error: %s : %s" % (self.TEST_PARAMETERS['PATH_TO_TEMP_DIRECTORY'], e.strerror))
        
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
                
  
def WorkflowSuite():
    suite = unittest.TestSuite()
    suite.addTest(WorkflowMoveToS3RestoreFromS3('test_move_to_s3_and_get_from_s3'))
    return suite
            
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
    
if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(SmallCanarySuite())
