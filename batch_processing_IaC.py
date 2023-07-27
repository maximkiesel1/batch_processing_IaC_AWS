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


# Function to check if a KMS key with a certain description exists
def check_kms_key(description):
    response = kms_client.list_keys()
    for key in response['Keys']:
        key_metadata = kms_client.describe_key(KeyId=key['KeyArn'])
        if 'Description' in key_metadata['KeyMetadata'] and key_metadata['KeyMetadata']['Description'] == description:
            return key['KeyArn']
    return None


# Function to create a KMS key if it doesn't exist
def create_kms_key_if_needed(description):
    kms_key_arn = check_kms_key(description)
    if not kms_key_arn:
        # Create the key
        response = kms_client.create_key(Description=description)
        kms_key_arn = response['KeyMetadata']['Arn']
        print(f'Created KMS key with ARN: {kms_key_arn}')
    else:
        print(f'Found existing KMS key with ARN: {kms_key_arn}')
    return kms_key_arn


# Function to attach kms policy
def attach_kms_policy_to_role(role_name, kms_key_arn):
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey"
                ],
                "Resource": kms_key_arn
            }
        ]
    }
    try:
        response = iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName='AllowUseOfKMSKey',
            PolicyDocument=json.dumps(policy)
        )
    except ClientError as e:
        print(f"Error attaching KMS policy to role {role_name}: {e}")



# Function to create an S3 bucket with kms encryption
def create_s3_bucket(bucket_name, kms_key_id):
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-north-1'
            }
        )
        print(f"S3 bucket {bucket_name} created successfully")

        # Activate a kms encryption
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'aws:kms',
                            'KMSMasterKeyID': kms_key_id
                        },
                    },
                ]
            }
        )
        print(f"Server-side encryption enabled on {bucket_name} with KMS key {kms_key_id}")
    except ClientError as e:
        print(f"Error: {e}")


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
def create_glue_role(role_name, s3_policy_arn, service):
    role_arn = does_iam_role_exist(role_name)
    if not role_arn:
        try:
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": service},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            response = iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
            )
            iam_client.attach_role_policy(RoleName=role_name, PolicyArn=s3_policy_arn)

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


def create_glue_security_configuration(security_configuration_name, kms_key_arn):
    try:
        response = glue_client.create_security_configuration(
            Name=security_configuration_name,
            EncryptionConfiguration={
                'S3Encryption': [{
                    'S3EncryptionMode': 'SSE-KMS',
                    'KmsKeyArn': kms_key_arn
                }],
                'CloudWatchEncryption': {
                    'CloudWatchEncryptionMode': 'DISABLED'
                },
                'JobBookmarksEncryption': {
                    'JobBookmarksEncryptionMode': 'DISABLED'
                }
            }
        )
        print(f"Glue security configuration {security_configuration_name} created successfully")
        return response['Name']
    except ClientError as e:
        print(f"Error creating Glue security configuration {security_configuration_name}: {e}")


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


# create s3 access policy
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


# Create a Glue job with KMS encryption
def create_glue_job(job_name, role_arn, script_path, source_bucket, target_bucket, security_configuration_name):
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
                Timeout=2880,
                # Add security configuration
                SecurityConfiguration=security_configuration_name  # Use the name of the security configuration
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


# Function to add inline policy to a role
def add_inline_policy_to_role(role_name, policy_name, policy_document):
    try:
        response = iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        print(f"Inline policy {policy_name} added to role {role_name} successfully")
    except ClientError as e:
        print(f"Error adding inline policy {policy_name} to role {role_name}: {e}")


# Create a CloudWatch Events rule to trigger the state machine execution monthly
def create_cloudwatch_events_rule(rule_name, state_machine_arn):
    try:
        # Create or update a CloudWatch Events rule with a cron schedule expression
        # This rule triggers every month
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression='cron(0 0 1 * ? *)',
            State='ENABLED'
        )
        # Extract the ARN of the created rule
        rule_arn = response['RuleArn']
        print(f"CloudWatch Events rule {rule_name} created successfully")

        # Create an IAM role for CloudWatch Events
        cloudwatch_events_role_arn = create_glue_role(
            'cloudwatch-events-processing-role',
            'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess',
            'events.amazonaws.com'
        )

        # Add inline policy to the role that allows it to start executions of the state machine
        policy_name = 'StartExecutionPolicy'
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "states:StartExecution",
                    "Resource": state_machine_arn  # The ARN of the state machine
                }
            ]
        }
        add_inline_policy_to_role('cloudwatch-events-processing-role', policy_name, policy_document)

        # Add the State Machine as a target for the rule
        # This means every time the rule triggers, it will start an execution of the state machine
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

        # Return the ARN of the created rule
        return rule_arn
    except ClientError as e:
        print(f"Error creating CloudWatch Events rule {rule_name}: {e}")


# Create cloudwatch event rule clocked in 5 minutes for testing
def create_cloudwatch_events_rule_5(rule_name, state_machine_arn):
    try:
        # Create or update a CloudWatch Events rule with a cron schedule expression
        # This rule triggers every 5 minutes
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression='rate(5 minutes)',
            State='ENABLED'
        )
        # Extract the ARN of the created rule
        rule_arn = response['RuleArn']
        print(f"CloudWatch Events rule {rule_name} created successfully")

        # Create an IAM role for CloudWatch Events
        cloudwatch_events_role_arn = create_glue_role(
            'cloudwatch-events-processing-role',
            'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess',
            'events.amazonaws.com'
        )

        # Add inline policy to the role that allows it to start executions of the state machine
        policy_name = 'StartExecutionPolicy'
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "states:StartExecution",
                    "Resource": state_machine_arn  # The ARN of the state machine
                }
            ]
        }
        add_inline_policy_to_role('cloudwatch-events-processing-role', policy_name, policy_document)

        # Add the State Machine as a target for the rule
        # This means every time the rule triggers, it will start an execution of the state machine
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

        # Return the ARN of the created rule
        return rule_arn
    except ClientError as e:
        print(f"Error creating CloudWatch Events rule {rule_name}: {e}")


# create cloudwatch event role
def create_cloudwatch_events_role(role_name, policy_arn, service, state_machine_arn):
    role_arn = does_iam_role_exist(role_name)
    if not role_arn:
        try:
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": service},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            response = iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
            )
            iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

            execution_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "states:StartExecution",
                        "Resource": state_machine_arn
                    }
                ]
            }

            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='StartExecutionToStepFunctions',
                PolicyDocument=json.dumps(execution_policy)
            )

            print(f"IAM-Rolle {role_name} erfolgreich erstellt")
            return response['Role']['Arn']
        except ClientError as e:
            print(f"Fehler beim Erstellen der IAM-Rolle {role_name}: {e}")
    else:
        print(f"IAM-Rolle {role_name} existiert bereits")
        return role_arn


# Create KMS id
kms_key_description = 'test'
kms_key_arn = create_kms_key_if_needed(kms_key_description)

# store it in a configuration file
with open('config.json', 'w') as f:
    json.dump({'KMS_KEY_ARN': kms_key_arn}, f)

security_configuration_name = "GlueSecurityConfiguration"
create_glue_security_configuration(security_configuration_name, kms_key_arn)

# Create S3 buckets
create_s3_bucket('data-ingestion-bucket-kiesel', kms_key_arn)  # Bucket for data ingestion
create_s3_bucket('pyspark-skript-bucket-kiesel', kms_key_arn)  # Bucket for PySpark script
create_s3_bucket('processing-bucket-kiesel', kms_key_arn)  # Bucket for processing

# Check if the S3 access policy exists, otherwise create one
s3_policy_name = 'S3AccessPolicy'
bucket_names = [
    'data-ingestion-bucket-kiesel',
    'pyspark-skript-bucket-kiesel',
    'processing-bucket-kiesel'
]

# Check if the S3 policy exists, if not create one
s3_policy_arn = does_s3_policy_exist(s3_policy_name)
if not s3_policy_arn:
    s3_policy_arn = create_s3_access_policy(s3_policy_name, bucket_names)

# Create an IAM role for AWS Glue and attach the S3 access policy
glue_role_name = 'glue_job_role_with_s3_access'
if s3_policy_arn:
    glue_role_arn = create_glue_role(glue_role_name, s3_policy_arn, "glue.amazonaws.com")
else:
    print("Failed to create or find the S3 access policy.")

attach_kms_policy_to_role(glue_role_name, kms_key_arn)

# Create an AWS Glue job
create_glue_job(
    'processing-job',
    glue_role_arn,
    's3://pyspark-skript-bucket-kiesel/pyspark_script.py',
    'data-ingestion-bucket-kiesel',
    'processing-bucket-kiesel',
    security_configuration_name)

# Create an IAM role for AWS Step Functions
step_functions_role_name = 'processing-step-functions-role'
step_functions_policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
step_functions_role_arn = create_glue_role(step_functions_role_name, step_functions_policy_arn, "states.amazonaws.com")

# Create state machine
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

# Call the function to create the state machine
state_machine_arn = create_state_machine(state_machine_name, role_arn, definition)
attach_kms_policy_to_role(step_functions_role_name, kms_key_arn)

# Create an IAM role for CloudWatch Events
cloudwatch_events_role_arn = create_cloudwatch_events_role(
    'cloudwatch-events-processing-role',
    'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess',
    'events.amazonaws.com',
    state_machine_arn  # Pass the ARN of the state machine to the function
)

attach_kms_policy_to_role('cloudwatch-events-processing-role', kms_key_arn)

# Add inline policy to the role that allows it to start executions of the state machine
policy_name = 'StartExecutionPolicy'
policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "states:StartExecution",
            "Resource": state_machine_arn  # The ARN of the state machine
        }
    ]
}
add_inline_policy_to_role('cloudwatch-events-processing-role', policy_name, policy_document)

# Define CloudWatch rule name
rule_name_5 = "5min-update-cloudwatch-events-rule"
rule_name = "monthly-update-cloudwatch-events-rule"

# Create or update the CloudWatch event rule
create_cloudwatch_events_rule_5(rule_name_5, state_machine_arn)
#create_cloudwatch_events_rule(rule_name, state_machine_arn)


