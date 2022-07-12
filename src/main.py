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

import argparse
import os.path
import subprocess
import sys

import singleton #Project Scoped Global Vars

'''
TO CONTROL PACKAGE VERSIONS. EACH LINE IN ../requirements.txt CAN TAKE ON ONE OF THE FOLLOWING FORMS:

Requirement	    Description
foo	            any version of foo
foo>=5	        any version of foo, above or equal to 5
foo>=5.6	    any version of foo, above or equal to 5.6
foo==5.6.1	    exact match
foo>5	        foo-5 or greater, including minor and patch
foo>5,<5.7	    foo-5 or greater, but less than foo-5.7
foo>0,<5.7	    any foo version less than foo-5.7
'''
def install_dependencies():
    install_command = subprocess.run(['pip3', 'install', '-r', f'{os.path.dirname(os.path.realpath(__file__))}/../requirements.txt'], capture_output=True)
    if install_command.returncode != 0:
        print("Dependencies \U0000274C", "stdout: " + install_command.stdout.decode('ascii'), "stderr: " + install_command.stderr.decode('ascii'), sep='\n') # unicode for RED X
        return False
    else:
        return True

'''
Creates parsers and enforces valid global parameter choices. Returns None if FSP should abort
'''
def arg_parse(args):
    # Highest level parser
    parser = argparse.ArgumentParser(description='Flexible Snapshot Proxy (FSP) CLI.')
    parser.add_argument("-o", "--origin_region", default=None, help="AWS Origin Region - source of Snapshots. (default: .aws/config then us-east-1)")
    parser.add_argument("-d", "--dry_run", default=False, action="store_true", help="Perform a dry run of FSP operation to check valid AWS permissions. (default: false)")
    parser.add_argument("-q", "--quiet", default=False, action="store_true", dest="q", help="Quiet output.")
    parser.add_argument("-v", "--verbosity", default=False, action="store_true", dest="v", help="Output verbosity. (Pass/Fail blocks per region)")
    parser.add_argument("-vv", default=False, action="store_true", dest="vv", help="Increased output verbosity. (Pass/Fail for individual blocks)")
    parser.add_argument("-vvv", default=False, action="store_true", dest="vvv", help="Maximum output verbosity. (All individual block retries will be recorded)")
    parser.add_argument("-nodeps", default=False, action="store_true", dest="nodeps", help="Do not verify/install dependencies.")
    
    # sub_parser for each CLI action
    subparsers = parser.add_subparsers(dest='command', title='EBS Playground Commands', description='First Positional Arguments. Additional help pages (-h or --help) for each command is available')
    list_parser = subparsers.add_parser('list', help='Returns accurate size of a Snapshot by enumerating actual consumed space')
    diff_parser = subparsers.add_parser('diff', help='Returns accurate size of a Snapshot Delta by enumerating the incremental difference between 2 snapshots with a common parent')
    download_parser = subparsers.add_parser('download', help='Transfers an EBS Snapshot to an arbitrary file or block device')
    deltadownload_parser = subparsers.add_parser('deltadownload', help='Downloads the delta between any two snapshots with a common parent')
    upload_parser = subparsers.add_parser('upload', help='Transfers an arbitrary file or block device to a new EBS Snapshot')
    copy_parser = subparsers.add_parser('copy', help='Transfers an EBS Snapshot to another EBS Direct API Endpoint (intended use case: copy Snapshots across accounts and/or regions)')
    sync_parser = subparsers.add_parser('sync', help='Synchronizes the incremental difference between 2 Snapshots, delta(A,B) to Snapshot C (clone of A), resulting in Snapshot D (clone of B)')
    movetos3_parser = subparsers.add_parser('movetos3', help='Transfers an EBS Snapshot to a customer-owned S3 Bucket (any S3 Storage Class) with zstandard compression, tuneable object size and an independent segment checksum')
    getfroms3_parser = subparsers.add_parser('getfroms3', help='Transfers a Snapshot stored in a customer-owned S3 Bucket to a new EBS snapshot')
    multiclone_parser = subparsers.add_parser('multiclone', help='Same functionality as “download”, but writing to multiple destinations in parallel')
    fanout_parser = subparsers.add_parser('fanout', help='Upload from file to multiple snapshot(s), provided a list of regions')
    
    # add CLI argument options for each command
    list_parser.add_argument('snapshot', help='Snapshot ID to list size of')
    
    diff_parser.add_argument('snapshot_one', help='First snapshot ID to used in comparison')
    diff_parser.add_argument('snapshot_two', help='Second snapshot ID to used in comparison')
    
    download_parser.add_argument('snapshot', help='Snapshot ID to download')
    download_parser.add_argument('file_path', help='File path of download location. (Absolute path preferred)')

    deltadownload_parser.add_argument('snapshot_one', help='First snapshot ID to used in comparison')
    deltadownload_parser.add_argument('snapshot_two', help='Second snapshot ID to used in comparison')
    deltadownload_parser.add_argument('file_path', help='File path of download location. (Absolute path preferred)')
    
    upload_parser.add_argument('file_path', help='File path of file or raw device to upload as snapshot')
    
    copy_parser.add_argument('snapshot', help='Snapshot ID to be copied')
    copy_parser.add_argument("-d", "--destination_region", required=False, default=None, help="AWS Destination Region. Where snapshot will copied to. (default: source region)")
    
    sync_parser.add_argument('snapshot_one', help='First snapshot ID to be synced (must share a parent snapshot with snapshotTwo)')
    sync_parser.add_argument('snapshot_two', help='Second snapshot ID to be synced (must share a parent snapshot with snapshotOne)')
    sync_parser.add_argument('destination_snapshot', help='The snapshot to synchronize')
    sync_parser.add_argument("-d", "--destination_region", default=None, help="AWS Destination Region. Where the destination snapshot exits. (default: source region)")
    sync_parser.add_argument("-f", "--full_copy", default=False, action="store_true", help="Does not make an size optimizations")
    
    movetos3_parser.add_argument('snapshot', help='Snapshot ID to be moved into an s3 bucket')
    movetos3_parser.add_argument('s3Bucket', help='The s3 bucket destination. Must be created within your AWS account')
    movetos3_parser.add_argument("-d", "--destination_region", default=None, help="AWS Destination Region. Where destination s3 bucket exists. (default: source region)")
    movetos3_parser.add_argument("-f", "--full_copy", default=False, action="store_true", help="Does not make an size optimizations")
    
    getfroms3_parser.add_argument('snapshot_prefix', help='The snapshot prefix specifying which snapshot to retrieve from s3 bucket')
    getfroms3_parser.add_argument('s3Bucket', help='The s3 bucket source. Must be created within your AWS account')
    getfroms3_parser.add_argument("-d", "--destination_region", default=None, help="AWS Destination Region. Region where retrieved snapshot will exist. (default: source region)")
    getfroms3_parser.add_argument("-f", "--full_copy", default=False, action="store_true", help="Does not make an size optimizations")
    
    multiclone_parser.add_argument('snapshot', help='Snapshot ID to multiclone')
    multiclone_parser.add_argument('file_path', help='File path to a .txt file containing list of multiclone destinations')

    fanout_parser.add_argument('devise_path', help='File path to raw device for fanout snapshot distributution')
    fanout_parser.add_argument('destinations', help='File path to a .txt file listing all regions the snapshot distributution on separate lines')
    
    args = parser.parse_args(args)
    
    '''
    Setup Global Variables
    '''
    import boto3 # Safe since this import is not reached unless dependency check passes.
    user_account = ''
    user_id = ''
    try:
        sts = boto3.client('sts')
        user_account = sts.get_caller_identity().get('Account')
        user_id = sts.get_caller_identity().get('UserId')
    except sts.exceptions as e:
        print("Can not get AWS user account. Is your AWS CLI Configured?")
        print("Try running: aws configure")
        print(e)
        return None

    user_canonical_id = ''
    try:
        s3 = boto3.client('s3')
        user_canonical_id = s3.list_buckets()['Owner']['ID']
    except s3.exceptions as e:
            print("Error: Could not get canonical user id")
            print(e)
            return None


    #Find aws regions
    aws_origin_region = args.origin_region
    if aws_origin_region == None:
        if boto3.session.Session().region_name is None:
            aws_origin_region = "us-east-1" # we assume us-east-1 if region is not configured in aws cli.
        else:
            aws_origin_region = boto3.session.Session().region_name
    
    if 'destination_region' in args and args.destination_region != None:
        aws_destination_region = args.destination_region
    else:
        aws_destination_region = aws_origin_region


    num_jobs = 0 
    if aws_origin_region == aws_destination_region:
        num_jobs = 16 # Snapshot gets split into N chunks, each of which is processed using N threads. Total complexity N^2.
    else:
        num_jobs = 27 # Increase concurrency for cross-region copies for better bandwidth.
                    # The value of 27 has been chosen because we appear to load-balance across 3 endpoints, so makes sense to use power of 3. 
                    # In testing, I was able to get 450MB/s between N.Virginia and Australia/Tokyo. 

    full_copy =  False
    if 'full_copy' in args:
        full_copy = True
    
    s3_bucket = None
    if 's3Bucket' in args:
        s3_bucket = args.s3Bucket

    verbosity = 0
    if args.q == True:
        verbosity = -1
    elif args.vvv == True:
        verbosity = 3
    elif args.vv == True:
        verbosity = 2
    elif args.v == True:
        verbosity = 1

    nodeps = False
    if args.nodeps == True:
        nodeps = True

    dry_run = args.dry_run

    # Validation of aws regions.
    aws_regions_list = []
    try:
        ec2 = boto3.client('ec2')
        rsp = ec2.describe_regions()["Regions"]
        for region in rsp:
            aws_regions_list.append(region["RegionName"])
    except Exception as e:
        print("Error attempting to validate AWS regions: %s" % e)
        return None
    if len(aws_regions_list) == 0:
        return None
    
    origin_is_valid = False
    destination_is_valid = False
    region_set = set()
    for region in aws_regions_list:
        region_set.add(region)
        if region == aws_origin_region:
            origin_is_valid = True
        if region == aws_destination_region:
            destination_is_valid = True
    if not (origin_is_valid==True and destination_is_valid==True):
        print("Invalid AWS region name(s) were provided")
        return None

    #Validate fanout regions
    aws_regions_fanout = []
    if args.command == 'fanout':
        f = open(args.destinations, 'r')
        for region in f:
            region = region.strip()
            if region == "":
                continue
            if region in region_set:
                aws_regions_fanout.append(region)
            else:
                print("Fanout - invalid AWS region name:", region)
                sys.exit(1) # Exit code for invalid parameters. Script cannot run

    args.destinations = aws_regions_fanout

    #Configure Global Vars
    singleton.init()
    singleton.AWS_ACCOUNT_ID = user_account
    singleton.AWS_USER_ID = user_id
    singleton.AWS_CANONICAL_USER_ID = user_canonical_id
    singleton.AWS_ORIGIN_REGION = aws_origin_region
    singleton.AWS_DEST_REGION = aws_destination_region
    singleton.NUM_JOBS = num_jobs
    singleton.FULL_COPY = full_copy
    singleton.S3_BUCKET = s3_bucket
    singleton.VERBOSITY_LEVEL = verbosity
    singleton.DRY_RUN = dry_run
    singleton.NODEPS = nodeps

    return args

if __name__ == "__main__":
    if sys.argv[1] != "-nodeps":
        if install_dependencies() == False:
            sys.exit(126) # Exit code for missing dependencies. Script cannot run
        print("Dependencies \U00002705") # unicode for GREEN CHECK

    args = arg_parse(sys.argv[1:])
    if args == None:
        print("\nExiting")
        sys.exit(1) # Exit code for invalid parameters. Script cannot run

    #Placing these imports earlier creates a circular dependency with the installer
    from fsp import list, diff, download, deltadownload, upload, copy, sync, movetos3, getfroms3, multiclone, fanout

    command = args.command
    if command == "list":
        list(snapshot_id=args.snapshot)
        
    elif command == "diff":
        diff(snapshot_id_one=args.snapshot_one, snapshot_id_two=args.snapshot_two)
        
    elif command == "download":
        download(snapshot_id=args.snapshot, file_path=args.file_path)

    elif command == "deltadownload":
        deltadownload(snapshot_id_one=args.snapshot_one, snapshot_id_two=args.snapshot_two, file_path=args.file_path)

    elif command == "upload":
        upload(file_path=args.file_path)
        
    elif command == "copy":
        copy(snapshot_id=args.snapshot)
        
    elif command == "sync":
        sync(snapshot_id_one=args.snapshot_one, snapshot_id_two=args.snapshot_two, destination_snapshot=args.destination_snapshot)
        
    elif command == "movetos3":
        movetos3(snapshot_id=args.snapshot)
        
    elif command == "getfroms3":
        getfroms3(snapshot_prefix=args.snapshot_prefix)
        
    elif command == "multiclone":
        multiclone(snapshot_id=args.snapshot, infile=args.file_path)
        
    elif command == "fanout":
        fanout(devise_path=args.devise_path, destination_regions=args.destinations)
    else:
        print("Unknown command: %s" % command)
        sys.exit(127) # Exit code for command not found. Script cannot run
