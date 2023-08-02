# batch_processing_IaC_AWS
This repository includes an Infrastructure as Code (IaC) to perform batch processing of big data.
The entire pipeline is designed with idempotency in mind, meaning it can be executed multiple times without causing disruptions or inconsistencies in the data processing. This is especially valuable as it not only ensures reliability and fault-tolerance of the system but also offers the flexibility of rerunning the pipeline whenever needed. Thus, even if the pipeline execution is repeated, it won't duplicate data or cause errors, but will always ensure that the most recent measurement data is correctly processed and stored for subsequent use.
This robust design is a key feature of the infrastructure that greatly enhances its resilience, making it well suited for production environments and critical applications.


## 1. Ingestion Layer
### Scheduling a Upload Script for S3 Execution Using a Bash Script and Cron

In terms of data upload, the Python script (`upload_csv_to_s3.py`), which is triggered by the bash script scheduled by cron, is designed to automatically select the most recent measurement data for each execution. This ensures that the newest data is always migrated to the cloud, keeping the AWS S3 bucket up-to-date with the latest temperature sensor readings.

1. Firstly, create a bash file that runs the script:

   ```bash
   #!/bin/bash
   /usr/bin/python3 /path/to/your/python/script/upload_csv_to_s3.py
   
Please replace /path/to/your/python/script with the exact path to the upload python file.

2. Make the bash file executable:

   ```bash
    chmod +x /path/to/your/python/script/upload_csv_to_s3.py
   
3. Configure cron to execute this bash file periodically. Open the crontab configuration with crontab -e and add a new line:

   ```bash
    0 0 1 * * /path/to/your/python/script/upload_csv_to_s3.py
   
This line schedules your script to run at 0:00 on the first day of every month. 

Please note: The exact path to Python (`/usr/bin/python3`) may vary depending on your system. You can find out the path to your Python installation by running which python3 in your terminal.

## 2. Storage/Processing Layer
The second layer is the Storage/Processing Layer. Here, the CSV file is stored in an AWS S3 bucket. Then, the data are processed using AWS Glue, a fully managed Extract-Transform-Load (ETL) service. The separate PySpark script `pyspark_skript` (stored in an S3 bucket) is utilized for ETL processing, allowing for flexible and scalable data transformations. The processed data are subsequently stored back in an S3 bucket as a Parquet file to be used for a machine learning application. During processing, the data are converted into a time-series format.
AWS Glue job for time series transformation is programmed to process the latest measurement data available in the S3 bucket.

## 3. Orchestration
The orchestration of the entire process is handled with AWS Step Functions and CloudWatch. Step Functions provide a serverless workflow orchestration service, enabling the coordination and sequencing of various AWS services. This results in high availability of the architecture, as failed workflows can be retried, or alternative paths can be defined. CloudWatch is used for scheduling the process, ensuring that the workflow is triggered monthly as per requirements.

## 4. Data Security
For data security, AWS IAM (Identity and Access Management) and AWS KMS (Key Management Service) are used. IAM is employed to control and restrict access to AWS resources, ensuring only authorized users or services can access data and services. KMS is used for data encryption, providing an additional layer of protection for sensitive information.
