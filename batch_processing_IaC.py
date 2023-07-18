import json
import boto3
from botocore.exceptions import ClientError

# Create an S3 client
s3_client = boto3.client('s3',
                         region_name="eu-north-1"
                         )

# Create an IAM client
iam_client = boto3.client('iam',
                          region_name="eu-north-1"
                          )

# Create a Glue client
glue_client = boto3.client('glue',
                           region_name="eu-north-1"
                           )

# Create a Step Functions client
step_functions_client = boto3.client('stepfunctions',
                                     region_name="eu-north-1"
                                     )

# Create a CloudWatch Events client
events_client = boto3.client('events',
                             region_name="eu-north-1"
                             )

# Create an AWS KMS client
kms_client = boto3.client('kms',
                          region_name="eu-north-1"
                          )


# Function to create an S3 bucket
def create_s3_bucket(bucket_name):
    if not does_s3_bucket_exist(bucket_name):
        try:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': 'eu-north-1'
                }
            )
            print(f"S3 bucket {bucket_name} created successfully")
        except ClientError as e:
            print(f"Error creating S3 bucket {bucket_name}: {e}")
    else:
        print(f"S3 bucket {bucket_name} already exists")


# Function to check if there is an existing s3
def does_s3_bucket_exist(bucket_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            return False
        elif error_code == '403':
            raise Exception(f'Forbidden access to bucket: {bucket_name}. Check AWS permissions and bucket policies.')
        else:
            raise
    return True


# Create an IAM role for AWS Glue and attach the S3 access policy
def create_glue_role(role_name, s3_policy_arn):
    role_arn = does_iam_role_exist(role_name)
    if not role_arn:
        try:
            response = iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument="""{
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "glue.amazonaws.com"},
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }""",
            )
            iam_client.attach_role_policy(RoleName=role_name, PolicyArn=s3_policy_arn)

            # Attach the CloudWatch logs policy to the role
            logging_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                        "Resource": "arn:aws:logs:*:*:*"
                    }
                ]
            }

            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='LoggingToCloudWatch',
                PolicyDocument=json.dumps(logging_policy)
            )

            print(f"IAM role {role_name} created successfully")
            return response['Role']['Arn']
        except ClientError as e:
            print(f"Error creating IAM role {role_name}: {e}")
    else:
        print(f"IAM role {role_name} already exists")
        return role_arn


# Function to check if there is an existing IAM role
def does_iam_role_exist(role_name):
    try:
        response = iam_client.get_role(RoleName=role_name)
        return response['Role']['Arn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            return None
        else:
            raise e


# Function to check if an S3 access policy exists
def does_s3_policy_exist(policy_name):
    try:
        response = iam_client.get_policy(PolicyArn=f"arn:aws:iam::{iam_client.get_user()['User']['Arn'].split(':')[4]}:policy/{policy_name}")
        return response['Policy']['Arn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            return None
        else:
            raise e


def create_s3_access_policy(policy_name, bucket_names):
    bucket_resources = []
    for bucket_name in bucket_names:
        bucket_resources.append(f"arn:aws:s3:::{bucket_name}/*")
        bucket_resources.append(f"arn:aws:s3:::{bucket_name}")

    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": bucket_resources
            }
        ]
    }

    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        return response['Policy']['Arn']
    except ClientError as e:
        print(f"Error creating policy {policy_name}: {e}")
        return None


# Create an AWS Glue job
def create_glue_job(job_name, role_arn, script_path, source_bucket, target_bucket):
    glue_job = get_glue_job(job_name)
    if not glue_job:
        try:
            response = glue_client.create_job(
                Name=job_name,
                Role=role_arn,
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': script_path,
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--job-bookmark-option': 'job-bookmark-enable',
                    '--S3_BUCKET_SOURCE': source_bucket,
                    '--S3_BUCKET_TARGET': target_bucket
                },
                GlueVersion='2.0',
                MaxRetries=0,
                Timeout=2880
            )
            print(f"Glue job {job_name} created successfully")
            return response['Name']
        except ClientError as e:
            print(f"Error creating Glue job {job_name}: {e}")
    else:
        print(f"Glue job {job_name} already exists")
        return glue_job['Name']


# Function to check if there is an exisiting glue job
def get_glue_job(job_name):
    try:
        response = glue_client.get_job(JobName=job_name)
        return response['Job']
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return None
        else:
            raise e


# Create a state machine for AWS Step Functions
def create_state_machine(state_machine_name, role_arn, definition):
    state_machine = get_state_machine(state_machine_name)
    if not state_machine:
        try:
            response = step_functions_client.create_state_machine(
                name=state_machine_name,
                definition=definition,
                roleArn=role_arn,
                type='STANDARD',
                loggingConfiguration={
                    'level': 'OFF',
                    'includeExecutionData': False,
                    'destinations': []
                }
            )
            print(f"State machine {state_machine_name} created successfully")
            return response['stateMachineArn']
        except ClientError as e:
            print(f"Error creating state machine {state_machine_name}: {e}")
            return None
    else:
        print(f"State machine {state_machine_name} already exists")
        return state_machine['stateMachineArn']


# Function to get a state machine
def get_state_machine(state_machine_name):
    try:
        state_machines = step_functions_client.list_state_machines()['stateMachines']
        for state_machine in state_machines:
            if state_machine_name in state_machine['name']:
                return state_machine
        return None
    except ClientError as e:
        print(f"Error retrieving state machine {state_machine_name}: {e}")
        return None


# Create a CloudWatch Events rule to trigger the state machine execution monthly
def create_cloudwatch_events_rule(rule_name, state_machine_arn):
    try:
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression='cron(0 0 1 * ? *)',
            State='ENABLED'
        )
        rule_arn = response['RuleArn']
        print(f"CloudWatch Events rule {rule_name} created successfully")

        # Erstellen Sie eine IAM-Rolle für CloudWatch Events
        cloudwatch_events_role_arn = create_glue_role(
            'cloudwatch-events-processing-role',
            'arn:aws:iam::aws:policy/service-role/AWS_Events_Invoke_Step_Functions_FullAccess'
        )

        # Fügen Sie die State Machine als Ziel für die Regel hinzu
        events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': state_machine_arn,
                    'RoleArn': cloudwatch_events_role_arn
                }
            ]
        )
        print(f"State machine {state_machine_arn} added as a target for the rule {rule_name}")

        return rule_arn
    except ClientError as e:
        print(f"Error creating CloudWatch Events rule {rule_name}: {e}")





# Create S3 buckets
create_s3_bucket('data-ingestion-bucket-kiesel')
create_s3_bucket('pyspark-skript-bucket-kiesel')
create_s3_bucket('processing-bucket-kiesel')


# Check if the S3 access policy exists, otherwise create one
s3_policy_name = 'S3AccessPolicy'
bucket_names = [
    'data-ingestion-bucket-kiesel',
    'pyspark-skript-bucket-kiesel',
    'processing-bucket-kiesel'
]

s3_policy_arn = does_s3_policy_exist(s3_policy_name)
if not s3_policy_arn:
    s3_policy_arn = create_s3_access_policy(s3_policy_name, bucket_names)

# Create an IAM role for CloudWatch Events
cloudwatch_events_role_arn = create_glue_role(
    'cloudwatch-events-processing-role',
    'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess'
)

# Create an IAM role for AWS Glue and attach the S3 access policy
glue_role_name = 'glue_job_role_with_s3_access'
if s3_policy_arn:
    glue_role_arn = create_glue_role(glue_role_name, s3_policy_arn)
else:
    print("Failed to create or find the S3 access policy.")

# Create an AWS Glue job
glue_job = create_glue_job(
    'processing-job',
    glue_role_arn,
    's3://pyspark-skript-bucket-kiesel/pyspark_script.py',
    'data-ingestion-bucket-kiesel',
    'processing-bucket-kiesel'
)

# Create an IAM role for AWS Step Functions
step_functions_role_arn = create_glue_role(
    'processing-step-functions-role',
    'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
)


state_machine_name = "processing-step-functions"
role_arn = step_functions_role_arn
definition = """
{
  "StartAt": "StartGlueJob",
  "States": {
    "StartGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "processing-job"
      },
      "End": true
    }
  }
}
"""

state_machine_arn = create_state_machine(state_machine_name, role_arn, definition)

# Create a CloudWatch Events rule to trigger the state machine execution monthly
create_cloudwatch_events_rule(
    'monthly-update-cloudwatch-events-rule',
    state_machine_arn
)