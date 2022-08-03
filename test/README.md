# Flexible Snapshot Proxy (FSP) Testing

The purpose of the unit tests in this directory are to ensure reliability and backwards compatibility is maintained throughout the development process. Currently testing accommodates all CLI actions checking the script I/O as well as checking resources provisioned by the script in AWS. 

There exists data validation tests when applicable for all CLI commands except: sync, fanout and getfroms3. There exist bug(s) in the testing scripts preventing these tests from passing (TODO: Investigate if issue in src/fsp.py or with testing setup!)

# Requirements

This testing script was designed to be run on an Linux instance, often as super user (sudo).
- This is because the testing script will use loopback devices (`/dev/loop*`) to emulate raw block devices

Other environments are likely to work with modification but are not supported currently.

*WARNING*: Costs to the AWS account will be accrued!

## Setup and Configuration
Once the EC2 environment has been setup. 

From the project root directory
 1. Setup the testing environment (test/config.yaml) and view different testing options using the command `python3 test/test.py --h`

## Running Tests
 2. Run tests with any or all of the following commands (note that `--snapshot_factory_checker` tests take a long time to run)
     - `sudo python3 test/test.py --all_tests`
     - `sudo python3 test/test.py --small_canary`
     - `python3 test/test.py --dependency_checker`
     - `sudo python3 test/test.py --snapshot_factory_checker`

## Issues
If any bugs are noticed feel free to open an issue on this GitHub repository
