# EBS Playground Testing Library

The purpose of the unit tests in this directory are to ensure backwards compatibility is maintained throughout the development process. Currently testing accommodates all CLI actions checking the script I/O as well as resource provisioning in AWS


# Requirements

This testing script was developed on an AWS EC2 instance that runs the Amazon Cloud9 code editor. Other environments are likely to work with modification but are not supported currently. [Here](https://docs.aws.amazon.com/cloud9/latest/user-guide/create-environment-main.html) is a guide to setup such an environment. 
WARNING: Costs to the AWS account will be accrued!

## Setup and Configuration
Once the EC2 environment has been setup. 

 1. Create a snapshot of your EC2 AMI (likely path: `/dev/xvda`). [Here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-snapshot.html) is a guide for creating snapshots from the AWS Management Console
 2. Create a private s3 bucket to store snapshots for the testing script. [Here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) is a guide for creating an s3 bucket
 3. Add a small EBS volume (< 10 GB) for the testing script. Larger volumes increase the time it takes to complete the script. Create a symbolic link from the volume to the test directory with a command similar to: `$ ln -s /dev/nvme1n1 /home/ec2-user/environment/ebs-playground/test/small_volume` . Where `/dev/nvme1n1` is the new volume device path which was found with `$ lsblk`
 4. Create 3 snapshots of the new EBS Volume where the volume is empty, half full and completely full. Use randomly generated data to fill the drive at each increment. All 3 snapshots will be required to run the testing script. Example commands to fill the random data would be `head -c 500M /dev/urandom > /dev/nvme1n1 ` and `head -c 1G /dev/urandom > /dev/nvme1n1`  for a 1GB EBS volume
 5. Open the file `ebs-playground/test/full_stack/config.yaml` and replace the placeholder values with the correct AWS resource identifiers, EC2 system absolute paths and other configurations for the testing script
 6. Install the following packages: `pip3 install unittest os boto3 yaml math time shutil`

## Running Tests

Use the command (from the top level directory): `python ./test/full_stack/tests.py`

## Issues
If any bugs are noticed feel free to open an issue on this Github repo

