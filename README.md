# DE_project_warehouses_retail_company
## Data Processing Pipeline

## Overview
This project implements a data processing pipeline that leverages Apache Spark and AWS services to automate the ingestion, processing, and storage of sales data from CSV files. The pipeline is designed to enhance data quality by validating schemas, transforming data, and producing data marts for analytics.

### Features
- Downloading CSV files from AWS S3.
- Schema validation to ensure data integrity.
- Error logging and handling for missing or malformed data.
- Data transformation and enrichment through joins with dimension tables.
- Writing results to both local storage and AWS S3 in Parquet format.
- Updating MySQL database staging tables with processing status.

## Technologies Used
- **Python**: The primary programming language.
- **Apache Spark**: For large-scale data processing.
- **AWS S3**: For file storage and retrieval.
- **MySQL**: For relational data storage and metadata management.
- **PySpark**: For data manipulation in a distributed environment.
- **Boto3**: AWS SDK for Python to interact with S3.

## Setup

### Prerequisites
1. **Python 3.10**: Make sure Python is installed on your machine. You can download it from [python.org](https://www.python.org/downloads/).
2. **Apache Spark**: Install Apache Spark on your local machine. Follow the instructions [here](https://spark.apache.org/downloads.html).
3. **AWS Account**: Create an AWS account and configure your IAM user with S3 access.
4. **MySQL Server**: Set up a MySQL server instance and create a database for staging tables.
5. **Required Libraries**: Install the necessary Python libraries:
   ```bash
   pip install pyspark boto3 mysql-connector-python

### Configuration
Modify the resources/dev/config.py file to set your configuration:

•	aws_access_key: Your AWS access key.

•	aws_secret_key: Your AWS secret key.

•	bucket_name: The name of your S3 bucket.

•	local_directory: The local directory for downloading files.

•	database_name: Your MySQL database name.

•	product_staging_table: The name of your staging table in MySQL.

•	mandatory_columns: A list of mandatory columns required in the CSV files.

•	Additional configuration variables for error handling and data mart directories.

### Usage
Running the Pipeline
Execute the data processing pipeline with the following command:
bash
Copy code
python your_script_name.py
### Input Files
The pipeline expects CSV files located in the specified S3 bucket. Ensure that the CSV files have the required schema as defined in your configuration.
### Logging
The script logs actions and errors to the console, allowing you to monitor the processing flow. Ensure that the logger is properly configured in logging_config.py.
### Functionality
The script performs the following main tasks:
1.	S3 Client Initialization:
o	Creates an S3 client for file operations.
o	Lists available buckets and checks for CSV files.
2.	File Validation:
o	Validates the existence of local CSV files.
o	Checks the schema of CSV files against a set of mandatory columns.
3.	File Downloading:
o	Downloads files from the specified S3 bucket to a local directory.
4.	Error Handling:
o	Moves files with missing mandatory columns to an error directory.
o	Logs errors encountered during processing.
5.	Data Transformation:
o	Reads data from validated CSV files.
o	Joins data with various dimension tables to enrich the dataset.
6.	Data Mart Creation:
o	Writes processed data to data marts in both Parquet format and CSV format.
o	Saves results to specified directories in S3.
7.	Database Operations:
o	Inserts records into a MySQL staging table.
o	Updates the status of records based on processing results.
8.	Clean Up:
o Deletes local files after processing to save space.
o	Moves processed files in S3 to a designated processed directory.
### Example
To see how the pipeline processes data, you can run the script and observe the logs for information about the files processed, any errors encountered, and the results written to S3.
### Contributing
Contributions are welcome! If you have suggestions or improvements, please create a pull request or open an issue.
### Acknowledgments
Thanks to the contributors of Apache Spark and the libraries used in this project. Special thanks to AWS for providing the infrastructure for this project.

