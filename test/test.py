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

import unittest
import os
import boto3
import yaml
import argparse
import sys

sys.path.insert(1, f'{os.path.dirname(os.path.realpath(__file__))}/../src')
from main import install_dependencies
from snapshot_factory import generate_pattern_snapshot

#import the tests
import test_functional
import test_unit

def generate_config(params):
    if params == None:
        params = {
        "aws-default-region": "xx-xxxx-x",
        "aws-cross-region": "xx-xxxx-x",
        "backoff-time-seconds": 3,
        "max-backoff-retry": 5,
        "s3-bucket": "xxxxxxxxxx",
        "snapshots": None
    }
    with open('config.yaml', 'w') as config_file:
        yaml.dump(params, config_file)

def setup():
    print("Setting up testing configuration file and testing resources...")
    
    BACKOFF_TIME = 3
    MAX_RETRY_COUNT = 5

    AWS_DEFAULT_REGION = "us-east-1"
    AWS_CROSS_REGION = "ap-southeast-2"

    # validate aws cli is configured correctly (testing dependency)
    try:
        sts = boto3.client("sts")
        user_account = sts.get_caller_identity().get("Account")
        user_id = sts.get_caller_identity().get("UserId")
    except sts.exceptions as e:
        print("Can not get AWS user account. Is your AWS CLI Configured?")
        print("Try running: aws configure")
        print(e)
        return None
    finally:
        if user_account is None or user_id is None:
            print("Can not get AWS user account. Is your AWS CLI Configured?")
            print("Try running: aws configure")
            print(e)
            return None

    s3_BUCKET = input("s3 bucket for testing (NON-PRODUCTION!): ") 
    
    # Generate test snapshots
    result_every_sector = generate_pattern_snapshot()
    result_every_even_sector = generate_pattern_snapshot(skip = 2)
    result_every_fourth_sector = generate_pattern_snapshot(skip = 4)
    result_every_third_sector = generate_pattern_snapshot(skip = 3)

    params = {
        "aws-default-region": AWS_DEFAULT_REGION,
        "aws-cross-region": AWS_CROSS_REGION,
        "backoff-time-seconds": BACKOFF_TIME,
        "max-backoff-retry": MAX_RETRY_COUNT,
        "s3-bucket": s3_BUCKET,
        "snapshots":{
            "every-sector": {
                "snapshot-id": result_every_sector["snap"],
                "size": result_every_sector["size"],
                "metadata": result_every_sector["metadata"]
            },
            "every-second-sector": {
                "snapshot-id": result_every_even_sector["snap"],
                "size": result_every_even_sector["size"],
                "metadata": result_every_even_sector["metadata"]
            },
            "every-fourth-sector": {
                "snapshot-id": result_every_fourth_sector["snap"],
                "size": result_every_fourth_sector["size"],
                "metadata": result_every_fourth_sector["metadata"]
            },
            "every-third-sector": {
                "snapshot-id": result_every_third_sector["snap"],
                "size": result_every_third_sector["size"],
                "metadata": result_every_third_sector["metadata"]
            },
        }
    }

    generate_config(params)




def parse_args(args):
    parser = argparse.ArgumentParser(description='Test Selector for Flexible Snapshot Proxy (FSP).\nPlease select one or many test suites to run.')
    parser.add_argument('--all_tests', default=False, action='store_true', help='Run all tests listed below.')
    parser.add_argument('--small_canary', default=False, action='store_true', help='Run tests on small data size for a sanity check that script is functional')
    parser.add_argument('--dependency_checker', default=False, action='store_true', help="Run tests to ensure that script dependency checker and installer is working correctly")
    parser.add_argument('--snapshot_factory_checker', default=False, action='store_true', help="Run tests to ensure that script to generate and check test snapshots is working correctly")

    return parser.parse_args(args)

if __name__ == '__main__':
    if not os.path.exists(f'{os.path.dirname(os.path.realpath(__file__))}/config.yaml'):
        if os.getuid() != 0:
            print("MUST RUN AS SUPER USER (SUDO)")
            sys.exit(1)
        setup()

    sys.exit(1)
    to_test = parse_args(sys.argv[1:])
    runner = unittest.TextTestRunner(verbosity=2)

    '''
    Note: Run tests in order of faster to lowest building dependency on each other

    e.g. if the dependency checker fails then no other tests will pass
    if small canary tests fail then no need to run larger or more extensive tests
    '''
    if to_test.all_tests or to_test.dependency_checker:
        print("\nTesting Dependency Checker:")
        result = runner.run(test_unit.DependencyCheckerSuite())
        print(f"{result.testsRun} tests were run - {len(result.skipped)} tests skipped.") 
        print(f"{len(result.errors)} Errors. {len(result.failures)} Failures")
    if to_test.all_tests or to_test.snapshot_factory_checker:
        print("\nTesting FSP with Small Canary Tests:")
        result = runner.run(test_unit.SnapshotFactorySuite())
        print(f"{result.testsRun} tests were run - {len(result.skipped)} tests skipped.")
        print(f"{len(result.errors)} Errors. {len(result.failures)} Failures")
    if to_test.all_tests or to_test.small_canary:
        print("\nTesting FSP with Small Canary Tests:")
        result = runner.run(test_functional.SmallCanarySuite())
        print(f"{result.testsRun} tests were run - {len(result.skipped)} tests skipped.")
        print(f"{len(result.errors)} Errors. {len(result.failures)} Failures")
