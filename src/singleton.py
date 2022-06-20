#Singleton module to store project scoped global variables


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
