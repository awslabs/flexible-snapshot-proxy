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
from os.path import exists
from datetime import datetime, timedelta

from singleton import SingletonClass #Project Scoped Global Vars

"""
Works like java comparator
v1 < v2 => return < 0
v1 == v2 => return == 0
v1 > v2 => return > 0
"""
def version_cmp(v1, v2):
    v1_t = tuple()
    v2_t = tuple()
    try:
        v1_t = tuple(map(int, (v1.split("."))))
        v2_t = tuple(map(int, (v2.split("."))))
    except ValueError:
        print("Versions are in incompatible format. Should be tag formatted (xx.xx.xx). Exiting...")
        sys.exit(1) # Exit code for invalid parameters. Script cannot run
    i = len(v1_t) - len(v2_t)
    if i < 0:
        zeros = -i*(0,)
        v1_t += zeros
    elif i > 0:
        zeros = i*(0,)
        v2_t += zeros

    less_than = v1_t <= v2_t
    greater_than = v1_t >= v2_t

    if less_than and greater_than:
        return 0
    elif less_than == True:
        return -1
    else:
        return 1

'''
TO CONTROL PACKAGE VERSIONS. EACH LINE IN ../requirements.txt CAN TAKE ON ONE OF THE FOLLOWING FORMS:

Case    Requirement	    Description
1       foo	            any version of foo [DO NOT DO THIS!]
2       foo>=5	        any version of foo, above or equal to 5
3       foo>=5.6	    any version of foo, above or equal to 5.6
4       foo==5.6.1	    exact match
5       foo>5	        foo-5 or greater, including minor and patch
6       foo>5,<5.7	    foo-5 or greater, but less than foo-5.7
7       foo>0,<5.7	    any foo version less than foo-5.7
'''

def dependency_checker(pip_freeze_output, requirements):
    requires = {} # Mapping <Package_Name>: {Min: XX.XX.XX or None, Max: XX.XX.XX or None} N.B. Version numbers are both inclusive

    # Mapping <Package_Name> |-> <Version_Number>
    to_install = {}
    to_fix_version = {}

    for line in requirements:
        if len(line.strip()) == 0: #Blank lines
            continue

        # Case 2, 3
        if ">=" in line:
            package, version = tuple(line.split(">=", 1))
            requires[package.strip()] = {"min": version.strip(), "max": None}
        # Case 4
        elif "==" in line:
            package, version = tuple(line.split("==", 1))
            requires[package.strip()] = {"min": version.strip(), "max": version.strip()}
        # Case 5
        elif ((">" in line) and (not "," in line)):
            package, version = tuple(line.split(">", 1))
            requires[package.strip()] = {"min": version.strip(), "max": None}
        # Case 6 & 7
        elif ">" in line and "," in line:
            package, versions = tuple(line.split(">", 1))
            version = version.strip()
            min, max = tuple(versions.split(",<", 1))
            requires[package.strip()] = {"min": min.strip(), "max": max.strip()}
        # Case 1
        else:
            print("Malformed requirement.txt file. All packages need version restrictions. Exiting...")
            sys.exit(1) # Exit code for invalid parameters. Script cannot run

    for line in pip_freeze_output:
        package = ""
        version = None
        if not ("==" in line): #No version packages. Odd but possible
            package = line.split(' ', 1)[0]
        else:
            package, version = tuple(line.split("==", 1))
            package.strip()
            version.strip()

        """
        3 Cases:
        1. Package not in requires => skip. User needs but this script does not
        2. Package in requires AND version ∈ [min, max] => remove package from requires.
        3. Package in requires AND version !∈ [min, max] => add package and max version to to_fix_version.

        Note: remaining packages in requires need to be installed as max supported version
        """
        if not package in requires:
            continue
        else:
            min_version = requires[package]["min"]
            max_version = requires[package]["max"]
            
            # is okay?
            if ((min_version == None and max_version == None)
            or (max_version == None and version_cmp(version, min_version)  >= 0)
            or ((version_cmp(version, min_version)  >= 0) and (version_cmp(version, max_version)  <= 0))):
                del requires[package]
                continue
            else:
                if max_version != None:
                    to_fix_version[package] = max_version
                else:
                    to_fix_version[package] = min_version
                del requires[package]

    for package in requires:
        if requires[package]["max"] != None:
            to_install[package] = requires[package]["max"]
        else:
            to_install[package] = requires[package]["min"]

    return to_install, to_fix_version

def install_dependencies(needs_install, needs_version_adjustment):
    print("Fixing Required Dependencies...")
    for package in needs_version_adjustment:
        if subprocess.run(['pip3', 'install', '-q', '--no-input', f'{package}=={needs_version_adjustment[package]}']).returncode != 0:
            return False
    
    for package in needs_install:
        if subprocess.run(['pip3', 'install', '-q', '--no-input', f'{package}=={needs_install[package]}']).returncode != 0:
            return False

    return True

'''
Creates parsers and enforces valid global parameter choices. Returns None if FSP should abort
'''
def arg_parse(args):
    # Highest level parser
    parser = argparse.ArgumentParser(description='Flexible Snapshot Proxy (FSP) CLI.')
    parser.add_argument("-o", "--origin_region", default="us-east-1", help="AWS Origin Region - source of Snapshots. (default: .aws/config then us-east-1)")
    parser.add_argument("-d", "--dry_run", default=False, action="store_true", help="Perform a dry run of FSP operation to check valid AWS permissions. (default: false)")
    parser.add_argument("-q", "--quiet", default=False, action="store_true", dest="q", help="Quiet output.")
    parser.add_argument("-v", "--verbosity", default=False, action="store_true", dest="v", help="Output verbosity. (Pass/Fail blocks per region)")
    parser.add_argument("-vv", default=False, action="store_true", dest="vv", help="Increased output verbosity. (Pass/Fail for individual blocks)")
    parser.add_argument("-vvv", default=False, action="store_true", dest="vvv", help="Maximum output verbosity. (All individual block retries will be recorded)")
    parser.add_argument("--nodeps", default=False, action="store_true", dest="nodeps", help="Do not verify/install dependencies.")
    parser.add_argument("--suppress_writes", default=False, action="store_true", help="Intended for underpowered devices. Will not write log files or check dependencies")
    
    # sub_parser for each CLI action
    subparsers = parser.add_subparsers(dest='command', title='Flexible Snapshot Proxy (FSP) Commands', description='First Positional Arguments. Additional help pages (-h or --help) for each command is available')
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
    copy_parser.add_argument("-d", "--destination_region", default=None, help="AWS Destination Region. Where snapshot will copied to. (default: source region)")
    
    sync_parser.add_argument('snapshot_one', help='First snapshot ID to be synced (must share a parent snapshot with snapshotTwo)')
    sync_parser.add_argument('snapshot_two', help='Second snapshot ID to be synced (must share a parent snapshot with snapshotOne)')
    sync_parser.add_argument('destination_snapshot', help='The snapshot to synchronize')
    sync_parser.add_argument("-d", "--destination_region", default=None, help="AWS Destination Region. Where the destination snapshot exits. (default: source region)")
    sync_parser.add_argument("-f", "--full_copy", default=False, action="store_true", help="Does not make an size optimizations")
    
    movetos3_parser.add_argument('snapshot', help='Snapshot ID to be moved into an s3 bucket')
    movetos3_parser.add_argument('s3Bucket', help='The s3 bucket destination. Must be created within your AWS account')
    movetos3_parser.add_argument("-d", "--destination_region", default=None, help="AWS Destination Region. Where target s3 bucket exists. (default: source region)")
    movetos3_parser.add_argument("-e", "--endpoint_url", default=None, help="S3 Endpoint URL, for custom destinations such as Snowball Edge. (default: none)")
    movetos3_parser.add_argument("-f", "--full_copy", default=False, action="store_true", help="Does not make an size optimizations")
    movetos3_parser.add_argument("-p", "--profile", default="default", help="Use a different AWS CLI profile, for custom destinations such as Snowball Edge.")
    
    getfroms3_parser.add_argument('snapshot_prefix', help='The snapshot prefix specifying which snapshot to retrieve from s3 bucket')
    getfroms3_parser.add_argument('s3Bucket', help='The s3 bucket source. Must be created within your AWS account')
    getfroms3_parser.add_argument("-d", "--destination_region", default=None, help="AWS Destination Region. Region where retrieved snapshot will exist. (default: source region)")
    getfroms3_parser.add_argument("-e", "--endpoint_url", default=None, help="S3 Endpoint URL, for custom destinations such as Snowball Edge. (default: none)")
    getfroms3_parser.add_argument("-f", "--full_copy", default=False, action="store_true", help="Does not make an size optimizations")
    getfroms3_parser.add_argument("-p", "--profile", default="default", help="Use a different AWS CLI profile, for custom destinations such as Snowball Edge.")
    
    multiclone_parser.add_argument('snapshot', help='Snapshot ID to multiclone')
    multiclone_parser.add_argument('file_path', help='File path to a .txt file containing list of multiclone destinations')

    fanout_parser.add_argument('device_path', help='File path to raw device for fanout snapshot distributution')
    fanout_parser.add_argument('destinations', help='File path to a .txt file listing all regions the snapshot distributution on separate lines')
    
    args = parser.parse_args(args)
    return args

def setup_singleton(args):
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
            session=boto3.Session(profile_name=singleton.AWS_S3_PROFILE)
            s3 = session.client('s3', endpoint_url=singleton.AWS_S3_ENDPOINT_URL)
            user_canonical_id = s3.list_buckets()['Owner']['ID']
    except s3.exceptions as e:
            print("Error: Could not get canonical user id")
            print(e)
            return None


    #Find aws regions
    aws_origin_region = args.origin_region
    if not (boto3.session.Session().region_name is None):
        aws_origin_region = boto3.session.Session().region_name
    
    if 'destination_region' in args and not (args.destination_region is None):
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
        full_copy = args.full_copy
    
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

    nodeps = args.nodeps
    suppress_writes = args.suppress_writes
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
    singleton.SUPPRESS_WRITES = suppress_writes

if __name__ == "__main__":
    global singleton
    singleton = SingletonClass()
    args = arg_parse(sys.argv[1:])

    if args == None:
        print("\nExiting")
        sys.exit(1) # Exit code for invalid parameters. Script cannot run

    timestamp_file = f"{os.path.dirname(os.path.realpath(__file__))}/../.fsp_deps_timestamp"
    if exists(timestamp_file):
        ctime = os.path.getctime(timestamp_file)
        ctime = datetime.fromtimestamp(ctime)
        now = datetime.now()
        delta = timedelta(days=7)
        if now < (ctime + delta): # If the timestamp_file was created within a week, skip dependencies check
            args.nodeps = True

    if args.nodeps == False:
        print("Checking Dependencies...")

        # Get users current packages
        result = subprocess.run(["pip3", "freeze"], capture_output=True)
        if result.returncode != 0:
            print("Cannot check your dependencies")
            sys.exit(1)
        pip_freeze_output = result.stdout.decode('utf-8').split('\n')

        # get requirements
        requirements = open(f"{os.path.dirname(os.path.realpath(__file__))}/../requirements.txt").readlines()
        requirements = [x.strip() for x in requirements]
        requirements = [x.replace(" ","") for x in requirements]

        needs_install, needs_version_adjustment = dependency_checker(pip_freeze_output, requirements)

        # Ask user for permission to install/upgrade/downgrade pip3 packages
        if len(needs_install) != 0 or len(needs_version_adjustment) != 0:
            print("Flexible Snapshot Proxy CLI Would like to make the following changes to your system pip3 packages:\n")
            if len(needs_install) != 0:
                print("\tInstall the following: <PACKAGE>==<VERSION>")
                for package in needs_install:
                    if needs_install[package] == None:
                        print(f"\t\t{package}")
                    else:
                        print(f"\t\t{package}=={needs_install[package]}")
            if len(needs_version_adjustment) != 0:
                print("\tUpgrade or Downgrade the following: <PACKAGE>==<VERSION>")
                for package in needs_version_adjustment:
                    if needs_version_adjustment[package] == None:
                        print(f"\t\t{package}")
                    else:
                        print(f"\t\t{package}=={needs_version_adjustment[package]}")
            
            choice = input("\nAgree to these changes? (y/n): ").strip()
            if choice == "y" or choice == "Y":
                if install_dependencies(needs_install, needs_version_adjustment) == False:
                    print("Failed to Install Dependencies\nExiting...")
                    sys.exit(1) # Exit code for invalid parameters. Script cannot run
            elif choice == "n" or choice == "N":
                print("No changes to system pip3 packages\nExiting...")
                sys.exit(1) # Exit code for invalid parameters. Script cannot run
            else:
                print("Invalid Input\nExiting...")
                sys.exit(1) # Exit code for invalid parameters. Script cannot run

        if exists(timestamp_file):
            os.remove(timestamp_file)
        open(timestamp_file, "a").close() # Create the timestamp file to cache dependencies check for 1 week
        
        print("Dependencies \U00002705") # unicode for GREEN CHECK
    
    setup_singleton(args)

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
        if not (args.endpoint_url is None):
            singleton.AWS_S3_ENDPOINT_URL = args.endpoint_url
        if not (args.endpoint_url is None):
            singleton.AWS_S3_PROFILE = args.profile
        movetos3(snapshot_id=args.snapshot)
        
    elif command == "getfroms3":
        if not (args.endpoint_url is None):
            singleton.AWS_S3_ENDPOINT_URL = args.endpoint_url
        if not (args.endpoint_url is None):
            singleton.AWS_S3_PROFILE = args.profile
        getfroms3(snapshot_prefix=args.snapshot_prefix)
        
    elif command == "multiclone":
        multiclone(snapshot_id=args.snapshot, infile=args.file_path)
        
    elif command == "fanout":
        fanout(device_path=args.device_path, destination_regions=args.destinations)
    else:
        print("Unknown command: %s" % command)
        sys.exit(127) # Exit code for command not found. Script cannot run
