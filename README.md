# Batch processing pipeline on AWS as Infrastructure as Code
![karosu-RIQdGGU2US0-unsplash](https://github.com/maximkiesel1/batch_processing_IaC_AWS/assets/119667336/a04806c5-e496-470e-a416-950eebfa480d)

This repository includes an Infrastructure as Code (`IaC`) to perform batch processing of big data (sensor data).
The entire pipeline is designed with idempotency in mind, meaning it can be executed multiple times without causing disruptions or inconsistencies in the data processing. This is especially valuable as it not only ensures reliability and fault-tolerance of the system but also offers the flexibility of rerunning the pipeline whenever needed. Thus, even if the pipeline execution is repeated, it won't duplicate data or cause errors, but will always ensure that the most recent measurement data is correctly processed and stored for subsequent use.
This robust design is a key feature of the infrastructure that greatly enhances its resilience, making it well suited for production environments and critical applications.

    
## 1. Ingestion Layer
### Scheduling a Upload Script for S3 Execution Using a Bash Script and Cron

In terms of data upload, the Python script (`upload_csv_to_s3.py`), which is triggered by the bash script scheduled by `cron`, is designed to automatically select the most recent measurement data for each execution. This ensures that the newest data is always migrated to the cloud, keeping the `AWS S3 Bucket` up-to-date with the latest sensor data.

1. Firstly, create a bash file that runs the script:

   ```bash
   #!/bin/bash
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
