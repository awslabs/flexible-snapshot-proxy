import argparse
import os.path
import subprocess
import sys
import json

import singleton #Project Scoped Global Vars

'''
Implemented like Java str compare.
if v1 > v2 |-> return > 0
if v1 == v2 |-> return == 0
if v1 < v2 |-> return < 0
'''
def version_cmp(v1, v2):

    arr1 = v1.split(".")
    arr2 = v2.split(".")
    n = len(arr1)
    m = len(arr2)
     
    arr1 = [int(i) for i in arr1]
    arr2 = [int(i) for i in arr2]
  
    if n>m:
      for i in range(m, n):
         arr2.append(0)
    elif m>n:
      for i in range(n, m):
         arr1.append(0)
     
    # returns 1 if version 1 is bigger and -1 if
    # version 2 is bigger and 0 if equal
    for i in range(len(arr1)):
      if arr1[i]>arr2[i]:
         return 1
      elif arr2[i]>arr1[i]:
         return -1
    return 0

def install_dependencies(upgrade_list, install_list):
    if len(install_list) > 0:
        print("\n\nThe Following Packages Need To Be Installed To Run Flexible Snapshot Proxy")
        for package in install_list:
            try:
                approval = input(f"pip3 install {package}? (Y/n): ")
                if approval == "Y" or approval == "y" or approval == "":
                    subprocess.run(["pip3", "install", package])
                elif approval == "N" or approval == "n":
                    return #Main will reject script for invalid dependencies
                else:
                    print("Unrecognized input. Aborting...")
                    sys.exit(1)
            except Exception as e:
                print("System Error\n", e)
                return 

    if len(upgrade_list) > 0:
        print("\n\nThe Following Packages Need To Be Updated To Run Flexible Snapshot Proxy")
        for package in upgrade_list:
            try:
                approval = input(f"pip3 install -U {package}? (Y/n): ")
                if approval == "Y" or approval == "y" or approval == "":
                    subprocess.run(["pip3", "install", "-U", package])
                elif approval == "N" or approval == "n":
                    return #Main will reject script for invalid dependencies
                else:
                    print("Unrecognized input. Aborting...")
                    sys.exit(1)
            except Exception as e:
                print("System Error\n", e)
                return 


def check_dependencies(prompt_install=False):
    dependency_file = os.path.dirname(os.path.realpath(__file__)) + '/../dependency.txt'
    required_dependencies = {}
    try:
        with open(dependency_file) as f:
            lines = f.readlines()

            for line in lines:
                split = line.split('==')
                package = split[0].strip()
                version = split[-1].strip()

                required_dependencies[package] = version
            
    except FileNotFound as e:
        print(f"The file {dependency_file} was not found in path", file=sys.stderr)
        print(e, file=sys.stderr)
        return False

    except Exception as e:
        print(f"The file '{dependency_file}' is not formatted correctly.", file=sys.stderr)
        print("All lines should be of the form: <package>==<version>", file=sys.stderr)
        print("System Error\n", e, file=sys.stderr)
        return False

    cur_dependencies = {}
    try:
        output = (subprocess.run(["pip3", "freeze"], capture_output=True).stdout).decode('utf-8')
        lines = output.split("\n")
    
        for line in lines:
            line = str(line)
            split = line.split('==')
            package = split[0].strip()
            version = split[-1].strip()
            if len(package) == 0 or len(version) == 0:
                continue

            cur_dependencies[package] = version

    except subprocess.CalledProcessError as e:
        print(e.output, file=sys.stderr)
        return False
    except Exception as e:
        print(e, file=sys.stderr)
        return False

    upgrade_list = []
    install_list = []
    for dependency in required_dependencies:
        version = required_dependencies[dependency]
        if dependency in cur_dependencies:
            #Exists, check version
            if version_cmp(version, cur_dependencies[dependency]) < 0:
                upgrade_list.append(dependency)
        else:
            #Doesn't exist. Needs installation
            install_list.append(dependency)

    if len(upgrade_list) == 0 and len(install_list) == 0:
        return True
    else:
        if prompt_install==True:
            install_dependencies(upgrade_list, install_list)
            return check_dependencies() #If installation has occurred, check once more then pass or fail
        else: 
            return False

'''
Creates parsers and enforces valid global parameter choices. Returns None if FSP should abort
'''
def arg_parse():
    # Highest level parser
    parser = argparse.ArgumentParser(description='EBS DirectAPI Client.')
    parser.add_argument("-o", "--origin_region", default=None, help="AWS Origin Region. Where snapshots will be found from. (default: .aws/config then us-east-1)")
    parser.add_argument("-d", "--dry_run", default=False, action="store_true", help="Preform a dry run of FSP operation to check valid AWS permissions. (default: false)")
    parser.add_argument("-q", "--quiet", default=False, action="store_true", dest="q", help="quiet output")
    parser.add_argument("-v", "--verbosity", default=False, action="store_true", dest="v", help="output verbosity. (Pass/Fail blocks per region)")
    parser.add_argument("-vv", default=False, action="store_true", dest="vv", help="increased output verbosity. (Pass/Fail for individual blocks)")
    parser.add_argument("-vvv", default=False, action="store_true", dest="vvv", help="Maximum output verbosity. (All individual block retries will be recorded)")
    
    # sub_parser for each CLI action
    subparsers = parser.add_subparsers(dest='command', title='EBS Playground Commands', description='First Positional Arguments. Additional help pages (-h or --help) for each command is available')
    list_parser = subparsers.add_parser('list', help='Returns accurate size of a Snapshot by enumerating actual consumed space')
    diff_parser = subparsers.add_parser('diff', help='Returns accurate size of a Snapshot Delta by enumerating the incremental difference between 2 snapshots with a common parent')
    download_parser = subparsers.add_parser('download', help='Transfers an EBS Snapshot to an arbitrary file or block device')
    upload_parser = subparsers.add_parser('upload', help='Transfers an arbitrary file or block device to a new EBS Snapshot')
    copy_parser = subparsers.add_parser('copy', help='Transfers an EBS Snapshot to another EBS Direct API Endpoint (intended use case: copy Snapshots across accounts and/or regions)')
    sync_parser = subparsers.add_parser('sync', help='Synchronizes the incremental difference between 2 Snapshots, delta(A,B) to Snapshot C (clone of A), resulting in Snapshot D (clone of B)')
    movetos3_parser = subparsers.add_parser('movetos3', help='Transfers an EBS Snapshot to a customer-owned S3 Bucket (any S3 Storage Class) with zstandard compression, tuneable object size and an independent segment checksum')
    getfroms3_parser = subparsers.add_parser('getfroms3', help='Transfers a Snapshot stored in a customer-owned S3 Bucket to a new EBS snapshot')
    multiclone_parser = subparsers.add_parser('multiclone', help=' Same functionality as “download”, but writing to multiple destinations in parallel')
    
    # add CLI argument options for each command
    list_parser.add_argument('snapshot', help='Snapshot ID to list size of')
    
    diff_parser.add_argument('snapshot_one', help='First snapshot ID to used in comparison')
    diff_parser.add_argument('snapshot_two', help='Second snapshot ID to used in comparison')
    
    download_parser.add_argument('snapshot', help='Snapshot ID to download')
    download_parser.add_argument('file_path', help='File path of download location. (Absolute path preferred)')
    
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
    multiclone_parser.add_argument('file_path', help='File path to file containing list of multiclone destinations')
    multiclone_parser.add_argument('-a', '--availability_zone', default=None, help='Availability Zone (AZ) to complete multiclone operation within')
    
    args = parser.parse_args()
    
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
        print("Can not get user account. Is your AWS CLI Configured?")
        print(e)
        return None

    user_canonical_id = ''
    if os.system("aws s3api list-buckets --query Owner.ID --output text > temp.txt") == 0:
        with open("temp.txt") as f:
            lines = f.readlines()
            user_canonical_id = lines[-1].strip()
    else:
        print("Can not get user account CANONICAL ID. Is your AWS CLI Configured?")
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

    dry_run = args.dry_run

    # Validation of aws regions.
    bash_get_aws_regions = subprocess.run(["aws", "ec2", "describe-regions", "--all-regions"], capture_output=True)
    if bash_get_aws_regions.returncode != 0:
        print("Can not retrieve AWS regions from AWS CLI")
        return None
    aws_regions_list = json.loads(bash_get_aws_regions.stdout)['Regions']
    origin_is_valid = False
    destination_is_valid = False
    for region in aws_regions_list:
        if region['RegionName'] == aws_origin_region:
            origin_is_valid = True
        if region['RegionName'] == aws_destination_region:
            destination_is_valid = True
    if not (origin_is_valid==True and destination_is_valid==True):
        print("Invalid AWS region name(s) were provided")
        return None
    

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

    return args
    
if __name__ == "__main__":
    if check_dependencies(prompt_install=True) == False:
        print("Missing and required dependencies. \nExiting")
        sys.exit(126) # Exit code for missing dependencies. Script cannot run
    
    args = arg_parse()
    if args == None:
        print("\nExiting")
        sys.exit(1) # Exit code for invalid parameters. Script cannot run

    #Placing these imports earlier creates a circular dependency with the installer
    from fsp import list, diff, download, upload, copy, sync, movetos3, getfroms3, multiclone, fanout

    command = args.command
    if command == "list":
        list(snapshot_id=args.snapshot)
        
    elif command == "diff":
        diff(snapshot_id_one=args.snapshot_one, snapshot_id_two=args.snapshot_two)
        
    elif command == "download":
        download(snapshot_id=args.snapshot, file_path=args.file_path)
        
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
        fanout()
    else:
        print("Unknown command: %s" % command)
        sys.exit(127) # Exit code for command not found. Script cannot run