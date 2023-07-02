import json
import boto3
from botocore.exceptions import ClientError

# Create an S3 client
s3_client = boto3.client('s3')

# Create an IAM client
iam_client = boto3.client('iam')

# Create a Glue client
glue_client = boto3.client('glue')

# Create a Step Functions client
step_functions_client = boto3.client('stepfunctions')

# Create a CloudWatch Events client
events_client = boto3.client('events')

# Create an AWS KMS client
kms_client = boto3.client('kms')

# Function to create an S3 bucket
def create_s3_bucket(bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"S3 bucket {bucket_name} created successfully")
    except ClientError as e:
        print(f"Error creating S3 bucket {bucket_name}: {e}")


# Create an IAM role for AWS Glue
def create_glue_role(role_name, policy_arn):
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
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        print(f"IAM role {role_name} created successfully")
        return response['Role']['Arn']
    except ClientError as e:
        print(f"Error creating IAM role {role_name}: {e}")

# Create an AWS Glue job
def create_glue_job(job_name, role_arn, script_path, source_bucket, target_bucket):
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

# Create a state machine for AWS Step Functions
def create_state_machine(state_machine_name, role_arn, glue_job_name, lambda_function_arn):
    try:
        response = step_functions_client.create_state_machine(
            name=state_machine_name,
            definition= """{
                "StartAt": "Run Glue Job",
                "States": {
                    "Run Glue Job": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::glue:startJobRun.sync",
                        "Parameters": {
                            "JobName": glue_job_name
                        },
                        "End": True
                    }
                }
            }""",
            roleArn=role_arn,
            type='STANDARD'
        )
        print(f"State machine {state_machine_name} created successfully")
        return response['stateMachineArn']
    except ClientError as e:
        print(f"Error creating state machine {state_machine_name}: {e}")

# Create a CloudWatch Events rule to trigger the state machine execution monthly
def create_cloudwatch_events_rule(rule_name, state_machine_arn):
    try:
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression='cron(0 0 1 * ? *)',
            State='ENABLED',
            Targets=[
                {
                    'Id': '1',
                    'Arn': state_machine_arn
                }
            ]
        )
        print(f"CloudWatch Events rule {rule_name} created successfully")
        return response['RuleArn']
    except ClientError as e:
        print(f"Error creating CloudWatch Events rule {rule_name}: {e}")

# Create S3 buckets
create_s3_bucket('data-ingestion-bucket')
create_s3_bucket('pyspark-skript-bucket')
create_s3_bucket('processing-bucket')


# Create an IAM role for AWS Glue
glue_role_arn = create_glue_role('glue_job_role', 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole')

# Create an AWS Glue job
glue_job_name = create_glue_job(
    'processing-job',
    glue_role_arn,
    's3://pyspark-skript-bucket/mein_pyspark_skript.py',
    's3://data-ingestion-bucket',
    's3://processing-bucket'
)

# Create an IAM role for AWS Step Functions
step_functions_role_arn = create_glue_role(
    'mein-step-functions-rolle',
    'arn:aws:iam::aws:policy/service-role/AWSLambdaRole'
)

# Create a state machine for AWS Step Functions
state_machine_arn = create_state_machine(
    'mein-step-functions-state-machine',
    step_functions_role_arn,
    glue_job_name
)

# Create a CloudWatch Events rule to trigger the state machine execution monthly
create_cloudwatch_events_rule(
    'mein-monatlicher-cloudwatch-events-rule',
    state_machine_arn
)