import json
import boto3
from botocore.exceptions import ClientError
import os


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

# Create an AWS Step Functions-Client
sfn_client = boto3.client('stepfunctions', region_name='eu-north-1')

# File to save the KMS key
kms_key_file = 'config.json'


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
    if kms_key_arn:
        print(f'Found existing KMS key with ARN: {kms_key_arn}')
    else:
        # Create the key
        response = kms_client.create_key(Description=description)
        kms_key_arn = response['KeyMetadata']['Arn']
        print(f'Created KMS key with ARN: {kms_key_arn}')

        # Save the key to the file
        with open(kms_key_file, 'w') as file:
            json.dump({'KMS_KEY_ARN': kms_key_arn}, file)
            print(f'KMS key ARN saved to {kms_key_file}')

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
    if does_s3_bucket_exist(bucket_name):
        print(f"S3 bucket {bucket_name} already exists")
    else:
        try:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': 'eu-north-1'
                }
            )
            print(f"S3 bucket {bucket_name} created successfully")

            # Activate kms encryption
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
    if role_arn:
        print(f"IAM role {role_name} already exists")
        return role_arn
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


# Check if security config exist
def create_glue_security_configuration_if_not_exists(security_configuration_name, kms_key_arn):
    try:
        glue_client.get_security_configuration(Name=security_configuration_name)
        print(f"Glue security configuration {security_configuration_name} already exists")
    except glue_client.exceptions.EntityNotFoundException:
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
    except ClientError as e:
        print(f"Error checking Glue security configuration {security_configuration_name}: {e}")


# Function to check if there is an existing IAM role
def does_iam_role_exist(role_name):
    try:
        response = iam_client.get_role(RoleName=role_name)
        return response['Role']['Arn']
    except iam_client.exceptions.NoSuchEntityException:
        return None
    except ClientError as e:
        print(f"Unexpected error while checking the existence of IAM role {role_name}: {e}")
        return None


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
                    "s3:ListBucket",
                    "s3:DeleteObject"
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


# Create Glue job with KMS encryption
def create_glue_job(job_name, role_arn, script_path, source_bucket, target_bucket, security_configuration_name):
    glue_job = get_glue_job(job_name)
    if glue_job:
        print(f"Glue job {job_name} already exists")
        return glue_job['Name']
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
            SecurityConfiguration=security_configuration_name
        )
        print(f"Glue job {job_name} created successfully")
        return response['Name']
    except ClientError as e:
        print(f"Error creating Glue job {job_name}: {e}")


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


# Create state machine for AWS Step Functions
def create_state_machine(state_machine_name, role_arn, definition):
    state_machine_arn = does_state_machine_exist(state_machine_name)
    if not state_machine_arn:
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
        return state_machine_arn


# Function to check if state machine exist
def does_state_machine_exist(state_machine_name):
    try:
        state_machines = sfn_client.list_state_machines()['stateMachines']
        for state_machine in state_machines:
            if state_machine['name'] == state_machine_name:
                return state_machine['stateMachineArn']
        return None
    except ClientError as e:
        print(f"Unexpected error while checking the existence of state machine {state_machine_name}: {e}")
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
def create_cloudwatch_events_rule_monthly(rule_name, state_machine_arn):
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


# Check if inline policy exist
def does_inline_policy_exist(role_name, policy_name):
    try:
        response = iam_client.list_role_policies(RoleName=role_name)
        if policy_name in response['PolicyNames']:
            return True
        else:
            return False
    except ClientError as e:
        print(f"Error retrieving inline policies for the role {role_name}: {e}")
        return False


# Create cloudwatch event rule clocked in 5 minutes for testing
def create_cloudwatch_events_rule_5min(rule_name, state_machine_arn):
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

        # Create an IAM role for CloudWatch Events if it doesn't exist
        cloudwatch_events_role_arn = does_iam_role_exist('cloudwatch-events-processing-role')
        if not cloudwatch_events_role_arn:
            cloudwatch_events_role_arn = create_glue_role(
                'cloudwatch-events-processing-role',
                'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess',
                'events.amazonaws.com'
            )
        else:
            print(f"IAM role cloudwatch-events-processing-role already exists")

        # Add inline policy to the role that allows it to start executions of the state machine
        policy_name = 'StartExecutionPolicy'
        if not does_inline_policy_exist('cloudwatch-events-processing-role', policy_name):
            policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "states:StartExecution",
                        "Resource": state_machine_arn
                    }
                ]
            }
            add_inline_policy_to_role('cloudwatch-events-processing-role', policy_name, policy_document)
        else:
            print(f"Inline policy {policy_name} already exists")

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

            print(f"IAM role {role_name} successfully created")
            return response['Role']['Arn']
        except ClientError as e:
            print(f"Error creating IAM role {role_name}: {e}")
    else:
        print(f"IAM role {role_name} already exists")
        return role_arn


# Check if Cloudwatch Event exist
def does_cloudwatch_rule_exist(rule_name):
    try:
        response = events_client.describe_rule(Name=rule_name)
        return response['Arn']
    except events_client.exceptions.ResourceNotFoundException:
        return None
    except ClientError as e:
        print(f"Unexpected error while checking the existence of CloudWatch rule {rule_name}: {e}")
        return None


# Create KMS id and check if the key file exists
if os.path.exists(kms_key_file):
    with open(kms_key_file, 'r') as file:
        config = json.load(file)
        kms_key_arn = config.get('KMS_KEY_ARN')
        print(f'Loaded KMS key ARN from {kms_key_file}: {kms_key_arn}')
else:
    # Create KMS key if needed
    kms_key_description = 'batch_processing_3'
    kms_key_arn = create_kms_key_if_needed(kms_key_description)


# Create Glue security config
security_configuration_name = "GlueSecurityConfiguration"
create_glue_security_configuration_if_not_exists(security_configuration_name, kms_key_arn)


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


# Try to get the ARN of the S3 policy if it exists, otherwise create a new one
s3_policy_arn = does_s3_policy_exist(s3_policy_name)
if s3_policy_arn is None:
    s3_policy_arn = create_s3_access_policy(s3_policy_name, bucket_names)


# If the S3 policy was found or created successfully, try to create the IAM role
if s3_policy_arn is not None:
    glue_role_name = 'glue_job_role_with_s3_access'
    glue_role_arn = create_glue_role(glue_role_name, s3_policy_arn, "glue.amazonaws.com")
    if glue_role_arn is not None:
        attach_kms_policy_to_role(glue_role_name, kms_key_arn)
    else:
        print(f"Failed to create IAM role {glue_role_name}.")
else:
    print(f"Failed to create or find the S3 access policy {s3_policy_name}.")


# Try to create the AWS Glue job if the IAM role was created successfully
if 'glue_role_arn' in locals():
    create_glue_job(
        'processing-job',
        glue_role_arn,
        's3://pyspark-skript-bucket-kiesel/pyspark_script.py',
        'data-ingestion-bucket-kiesel',
        'processing-bucket-kiesel',
        security_configuration_name
    )


# Create an IAM role for AWS Step Functions if it doesn't exist
step_functions_role_name = 'processing-step-functions-role'
step_functions_role_arn = does_iam_role_exist(step_functions_role_name)

if step_functions_role_arn is None:
    step_functions_policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
    step_functions_role_arn = create_glue_role(step_functions_role_name, step_functions_policy_arn, "states.amazonaws.com")
    if step_functions_role_arn is not None:
        attach_kms_policy_to_role(step_functions_role_name, kms_key_arn)
    else:
        print(f"Failed to create IAM role {step_functions_role_name}.")
else:
    print(f"IAM role {step_functions_role_name} already exists")


# Create state machine if it doesn't exist
state_machine_name = "processing-step-functions"
state_machine_arn = does_state_machine_exist(state_machine_name)

if state_machine_arn is None:
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
    attach_kms_policy_to_role(step_functions_role_name, kms_key_arn)
else:
    print(f"State machine {state_machine_name} already exists")


# Create an IAM role for CloudWatch Events if it doesn't exist
cloudwatch_events_role_name = 'cloudwatch-events-processing-role'
cloudwatch_events_role_arn = does_iam_role_exist(cloudwatch_events_role_name)

if cloudwatch_events_role_arn is None:
    cloudwatch_events_role_arn = create_cloudwatch_events_role(
        cloudwatch_events_role_name,
        'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess',
        'events.amazonaws.com',
        state_machine_arn  # Pass the ARN of the state machine to the function
    )
    attach_kms_policy_to_role(cloudwatch_events_role_name, kms_key_arn)
else:
    print(f"IAM role {cloudwatch_events_role_name} already exists")


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

if not does_inline_policy_exist(cloudwatch_events_role_name, policy_name):
    add_inline_policy_to_role(cloudwatch_events_role_name, policy_name, policy_document)
else:
    print(f"Inline policy {policy_name} already exists in role {cloudwatch_events_role_name}")


# Create or update the CloudWatch event rule if it doesn't exist
rule_name = "cloudwatch-events-rule-monthly"
rule_arn = does_cloudwatch_rule_exist(rule_name)
if rule_arn is None:
    create_cloudwatch_events_rule_monthly(rule_name, state_machine_arn)
else:
    print(f"CloudWatch rule {rule_name} already exists")


