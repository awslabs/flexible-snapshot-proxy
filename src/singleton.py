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

def init():
    global AWS_ACCOUNT_ID # Account ID of the user. Retrieved using boto3 which reads AWS CLI config
    global AWS_USER_ID #Short account specific ID
    global AWS_CANONICAL_USER_ID #Long cross account unique ID
    global AWS_ORIGIN_REGION #Region where data originates from
    global AWS_DEST_REGION #Region where data is copied to
    global NUM_JOBS #Number jobs to be run in parallel
    global FULL_COPY #Create full copy of snapshot at additional cost (more through)
    global S3_BUCKET #S3 bucket where snapshots are stored or will be stored in
    global VERBOSITY_LEVEL #-1 quite. 1,2,3 for v, vv, vvv respectively
    global DRY_RUN #Run a FSP Action, only checking permissions

    global RETRY_BLOCK_COUNT, RETRY_RANGE_COUNT, RETRY_JOB_COUNT
    RETRY_BLOCK_COUNT = 10
    RETRY_RANGE_COUNT = 3
    RETRY_JOB_COUNT = 1 #Job is same as range
