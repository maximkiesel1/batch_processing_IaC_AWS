# batch_processing_IaC_AWS
This repository includes an Infrastructure as Code (IaC) to perform batch processing of big data.

### Scheduling a Upload Script for S3 Execution Using a Bash Script and Cron

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

Please note: The exact path to Python (/usr/bin/python3) may vary depending on your system. You can find out the path to your Python installation by running which python3 in your terminal.