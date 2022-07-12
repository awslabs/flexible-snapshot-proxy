# Flexible Snapshot Proxy (FSP) Testing

The purpose of the unit tests in this directory are to ensure reliability and backwards compatibility is maintained throughout the development process. Currently testing accommodates all CLI actions checking the script I/O as well as checking resources provisioned by the script in AWS.


# Requirements

This testing script was designed to be run on an Amazon EC2 instance.
- Should have networking configurations to allow for connections to Amazon S3 as well as EBS volumes and snapshots
- TODO: Decouple script dependency on EC2/VPC to allow it to run anywhere.

Other environments are likely to work with modification but are not supported currently.

*WARNING*: Costs to the AWS account will be accrued!

## Setup and Configuration
Once the EC2 environment has been setup. 

 1. Create a snapshot of your EC2 AMI (likely path: `/dev/xvda`). [Here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-snapshot.html) is a guide for creating snapshots from the AWS Management Console
 2. Create a private s3 bucket to store snapshots for the testing script. [Here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) is a guide for creating an s3 bucket
 3. Add a small EBS volume (< 10 GB) for the testing script. Larger volumes increase the time it takes to complete the script. Take note of the volume path in `/dev/EBS_VOLUME_HERE`
 4. Create 3 snapshots of the new EBS Volume where the volume is empty, half full and completely full. Use randomly generated data to fill the drive at each increment. All 3 snapshots will be required to run the testing script. Example commands to fill the random data would be `head -c 500M /dev/urandom > /dev/EBS_VOLUME_HERE` and `head -c 1G /dev/urandom > /dev/EBS_VOLUME_HERE`  for a 1GB EBS volume
 5. Run the python script `python3 flexible-snapshot-proxy/test/test.py` to create a `flexible-snapshot-proxy/config.yaml` file with correct AWS resource identifiers. Be sure to use EC2 system absolute paths and other configurations for the testing script
 6. Install the following packages in `flexible-snapshot-proxy/requirements.txt`

## Running Tests

Use the command (from the top level directory): `python3 flexible-snapshot-proxy/test/test.py` with optional parameters `--all_tests` or `--small_canary` or `--dependency_checker`. Depending on which testing suite you would like to run

## Issues
If any bugs are noticed feel free to open an issue on this Github repo

