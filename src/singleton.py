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

#Singleton module to store project scoped global variables - Should only be initialized in main.py the imported to any other module in src/


class SingletonClass(object):
    """ALL CLI ACTIONS SCOPED"""
    AWS_ACCOUNT_ID = None  # Account ID of the user. Retrieved using boto3 which reads AWS CLI config
    AWS_USER_ID = None  # Short account specific ID
    AWS_CANONICAL_USER_ID = None  # Long cross account unique ID
    AWS_ORIGIN_REGION = None  # Region where data originates from
    AWS_DEST_REGION = None  # Region where data is copied to
    NUM_JOBS = None  # Number jobs to be run in parallel
    FULL_COPY = None  # Create full copy of snapshot at additional cost (more through)
    S3_BUCKET = None  # S3 bucket where snapshots are stored or will be stored in
    VERBOSITY_LEVEL = None  # -1 quite. 1,2,3 for v, vv, vvv respectively
    DRY_RUN = None  # Run a FSP Action, only checking permissions
    ESTIMATE = None # Estimate cost
    NODEPS = None  # Skip Dependency Checks
    SUPPRESS_WRITES = None  # Script will not produce log files

    """"Some Project Scoped Constants"""
    RETRY_BLOCK_COUNT = 10
    RETRY_RANGE_COUNT = 3
    RETRY_JOB_COUNT = 1  # Job is same as range

    """"Global Variables for getfroms3 and movetos3"""
    ENDPOINT_URL = None
    PROFILE = None

    """Global Variables for getfroms3 and movetos3"""
    AWS_S3_ENDPOINT_URL = None
    AWS_S3_PROFILE = None

    """Dictionary of important quotas by API operation and Exception"""
    AWS_SERVICE_QUOTAS = {
      ("GetSnapshotBlock","ThrottlingException") : "L-C125AE42",
      ("GetSnapshotBlock","RequestThrottledException") : "L-028ACFB9",
      ("PutSnapshotBlock","ThrottlingException") : "L-AFAE1BE8",
      ("PutSnapshotBlock","RequestThrottledException") : "L-1774F84A",
    }

    def __new__(cls):
      if not hasattr(cls, 'instance'):
        cls.instance = super(SingletonClass, cls).__new__(cls)
      return cls.instance