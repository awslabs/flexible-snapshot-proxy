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

from random import choices
import unittest
import os
import boto3
import yaml
import argparse
import sys

sys.path.insert(1, f'{os.path.dirname(os.path.realpath(__file__))}/../src')
from main import install_dependencies

#import the tests
import test_functional
import test_unit

def generate_config(params):
    if params == None:
        params = {}
        params["ami-snapshot"] = "snap-xxxxxxxxxxx"
        params["dest-s3bucket"] = "xxxxxxxxxxx"
        params["ec2-instanceId"] = "i-xxxxxxxxxxx"
        params["original-ami-path"] = "/xxxxxxxxxx"
        params["restored-ami-path"] = "/xxxxxxxxxx"
        params["small-volume-path"] = "/xxxxxxxxxx"
        params["default-retry-time"] = 3
        params["max-retry-count"] = 5
        params["aws-origin-region"] = "xx-xxxx-x"
        params["small-volume-snapshots"] = {
            "empty": "snap-xxxxxxxxxxx",
            "half": "snap-xxxxxxxxxxx",
            "full": "snap-xxxxxxxxxxx",
        }
    with open('config.yaml', 'w') as config_file:
        yaml.dump(params, config_file)

def setup():
    print("Setting up testing configuration file...")
    print("Enter y/Y for a blank config file (for if required testing resources are setup, but config.yaml is lost)")
    choice = input("(N/y): ")
    if choice == 'y' or choice == 'Y':
        generate_config(None)
    else: #get inputs for config file
        print("Enter y/Y to configure aws resources required for testing: ")
        choice = input("(n/Y): ")
        if choice == 'n' or choice == 'N':
            aws_region = input(f"aws-origin-region: ")
            ec2_instance = input("EC2 instance id: ")
            ami_snapshot = input(f"EC2 instance {ec2_instance} ami snapshot: ")
            s3_bucket = input("Destination s3 bucket: ")
            original_ami_path = input(f"EC2 instance {ec2_instance} original ami path: ")
            restored_ami_path = input(f"EC2 instance {ec2_instance} restored ami path: ")
            small_volume_path = input(f"Small (1GiB) volume path on {ec2_instance}: ")
            empty_snap = input(f"Empty snapshot-id of small (1GiB) volume: ")
            half_snap = input(f"Half full snapshot-id of small (1GiB) volume: ")
            full_snap = input(f"Full snapshot-id of small (1GiB) volume: ")

            params = {}
            params["ami-snapshot"] = ami_snapshot
            params["dest-s3bucket"] = s3_bucket
            params["ec2-instanceId"] = ec2_instance
            params["original-ami-path"] = original_ami_path
            params["restored-ami-path"] = restored_ami_path
            params["small-volume-path"] = small_volume_path
            params["default-retry-time"] = 3
            params["max-retry-count"] = 5
            params["aws-origin-region"] = aws_region
            params["small-volume-snapshots"] = {
                "empty": empty_snap,
                "half": half_snap,
                "full": full_snap,
            }
            generate_config(params)
        else:
            print("This feature is not yet available!")



def parse_args(args):
    parser = argparse.ArgumentParser(description='Test Selector for Flexible Snapshot Proxy (FSP).\nPlease select one or many test suites to run.')
    parser.add_argument('--all_tests', default=False, action='store_true', help='Run all tests listed below.')
    parser.add_argument('--small_canary', default=False, action='store_true', help='Run tests on small data size for a sanity check that script is functional')
    parser.add_argument('--dependency_checker', default=False, action='store_true', help="Run tests to ensure that script dependency checker and installer is working correctly")

    return parser.parse_args(args)

if __name__ == '__main__':
    if not os.path.exists(f'{os.path.dirname(os.path.realpath(__file__))}/config.yaml'):
        setup()

    to_test = parse_args(sys.argv[1:])
    runner = unittest.TextTestRunner(verbosity=2)

    '''
    Note: Run tests in order of faster to lowest building dependency on each other

    e.g. if the dependency checker fails then no other tests will pass
    if small canary tests fail then no need to run larger or more extensive tests
    '''
    if to_test.all_tests or to_test.dependency_checker:
        print("\nTesting Dependency Checker:")
        runner.run(test_unit.DependencyCheckerSuite())
    if to_test.all_tests or to_test.small_canary:
        print("\nTesting FSP with Small Canary Tests:")
        runner.run(test_functional.SmallCanarySuite())
