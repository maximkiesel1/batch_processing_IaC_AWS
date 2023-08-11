# Batch processing pipeline on AWS as Infrastructure as Code
![karosu-RIQdGGU2US0-unsplash](https://github.com/maximkiesel1/batch_processing_IaC_AWS/assets/119667336/a04806c5-e496-470e-a416-950eebfa480d)

This repository includes an Infrastructure as Code (`IAC`) to perform batch processing of big data in AWS.
<img width="570" alt="Bildschirmfoto 2023-08-11 um 18 15 22" src="https://github.com/maximkiesel1/batch_processing_IaC_AWS/assets/119667336/574b3aea-66d5-434d-9e9b-9fb319e4e550">

The concept presented is based on an ecosystem in AWS and is implemented using Python and boto3 for `IAC` development to ensure maintainability of the architecture.

The first layer is the ingestion layer, which uses a Bash script and `cron` to migrate a CSV file to the cloud. The CSV file contains raw data from a temperature sensor. Only time intervals are recorded, which can include multiple readings (if the value has not changed but a measurement has been taken).

The second layer is the storage/processing layer. This is where the CSV file is stored in an `AWS S3 bucket`. The data is then processed using `AWS Glue`, a fully managed extract-transform-load (`ETL`) service. A separate `pyspark` script (stored in an `S3 bucket`) is used for `ETL` processing to provide flexible and scalable data transformations. The processed data is then stored back in an `S3 bucket` as a `parquet` file to be used by a machine learning application. During processing, the data is converted to a time series format and the entire process is orchestrated using `AWS Step Functions` and `CloudWatch`. `Step Functions` provides a serverless workflow orchestration service that enables the coordination and sequencing of various AWS services. This achieves high availability of the architecture by allowing failed workflows to be repeated or alternative paths to be defined. `CloudWatch` is used to schedule the process to ensure that the workflow is triggered monthly according to specifications. 

`AWS IAM` (Identity and Access Management) and `AWS KMS` (Key Management Service) are used for data security. `IAM` is used to control and restrict access to AWS resources to ensure that only authorised users or services can access data and services. `KMS` is used for data encryption to provide an additional layer of protection for sensitive information.

The entire pipeline is designed with idempotency in mind, meaning it can be executed multiple times without causing disruptions or inconsistencies in the data processing. This is especially valuable as it not only ensures reliability and fault-tolerance of the system but also offers the flexibility of rerunning the pipeline whenever needed. Thus, even if the pipeline execution is repeated, it won't duplicate data or cause errors, but will always ensure that the most recent measurement data is correctly processed and stored for subsequent use.
This robust design is a key feature of the infrastructure that greatly enhances its resilience, making it well suited for production environments and critical applications.

## Setting Up AWS Credentials for Using This IAC Repository with boto3
To use and deploy the Infrastructure as Code (`IAC`) structures contained in this repository, it's crucial to have valid AWS credentials configured. Here's a step-by-step guide:

1. **AWS Account**: 
- If you haven't already, sign up for an AWS account on the AWS Management Console.

2. **IAM User Creation**: 
- Navigate to the Identity and Access Management (`IAM`) dashboard.
- Create a new `IAM` user. 
- It's a best practice not to use your AWS root account for programmatic activities. 
- Grant the user appropriate permissions. For initial testing, you can grant AdministratorAccess but consider limiting permissions for production uses.

3. **Access Keys**: 
- Once the `IAM` user is created, generate the `Access Key ID` and `Secret Access Key`. 
- Store these keys in a safe and secure place. AWS only shows the secret key once, and it cannot be retrieved again.

4. **Configuring the AWS CLI**:
- If you haven't already, install the `AWS CLI`. 
   ```bash
   pip install awscli

- Configure the `CLI` by running `aws configure` in your terminal. This command prompts you to provide the `Access Key ID`, `Secret Access Key`, `default region`, and `default output format`.

5. **Configuring boto3**:
- `boto3` will automatically use the credentials stored from the `AWS CLI. 
- Alternatively, you can manually configure credentials in Python scripts:

```
import boto3
session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY',
    region_name='YOUR_REGION_NAME'
)
```

6. **Using Profiles**:
- For managing multiple AWS accounts or roles, consider using named profiles. 
- You can set these up during `aws configure` by using the `--profile` flag, e.g., `aws configure --profile myprofile`. 
- In `boto3`, you can then reference these named profiles:

```
session = boto3.Session(profile_name='myprofile')
```

By ensuring that your AWS credentials are set up properly and securely, you'll be prepared to execute the `IAC` code in this repository effectively and safely.

## 1. Ingestion Layer
### Scheduling a Upload Script for S3 Execution Using a Bash Script and Cron

In terms of data upload, the Python script (`upload_csv_to_s3.py`), which is triggered by the bash script scheduled by `cron`, is designed to automatically select the most recent measurement data for each execution. This ensures that the newest data is always migrated to the cloud, keeping the `AWS S3 Bucket` up-to-date with the latest sensor data.

1. Firstly, create a bash file that runs the script:

   ```bash
   /usr/bin/python3 /path/to/your/python/script/upload_csv_to_s3.py
   
Please replace `/path/to/your/python/script` with the exact path to the upload Python file.

2. Make the bash file executable:

   ```bash
    chmod +x /path/to/your/python/script/upload_csv_to_s3.py
   
3. Configure cron to execute this bash file periodically. Open the `crontab` configuration with `crontab -e` and add a new line:

   ```bash
    0 0 1 * * /path/to/your/python/script/upload_csv_to_s3.py
   
This line schedules your script to run at `0:00` on the first day of every month. 

Please note: The exact path to Python (`/usr/bin/python3`) may vary depending on your system. You can find out the path to your Python installation by running which `python3` in your terminal.

## 2. Storage/Processing Layer
The second layer is the Storage/Processing Layer. Here, the CSV file is stored in an `AWS S3 Bucket`. Then, the data are processed using `AWS Glue`, a fully managed Extract-Transform-Load (ETL) service. The separate PySpark script `pyspark_skript.py` (stored in an `S3 Bucket`) is utilized for ETL processing, allowing for flexible and scalable data transformations. The processed data are subsequently stored back in an `S3 Bucket` as a Parquet file to be used for a machine learning application. During processing, the data are converted into a time-series format.
`AWS Glue Job` for time series transformation is programmed to process the latest measurement data available in the `S3 Bucket`.

## 3. Orchestration
For orchestrating the `AWS Glue Job`, `AWS Step Functions` is employed. This allows for managing the sequence and coordination of the data processing tasks. A `CloudWatch Event` is set up to trigger this orchestrated process on a monthly basis. This mechanism ensures that the Glue job is executed regularly and seamlessly, enhancing the overall efficiency of the data processing pipeline.

## 4. Data Security
For data security, `AWS IAM` (Identity and Access Management) and `AWS KMS` (Key Management Service) are used. `IAM` is employed to control and restrict access to AWS resources, ensuring only authorized users or services can access data and services. `KMS` is used for data encryption, providing an additional layer of protection for sensitive information.
