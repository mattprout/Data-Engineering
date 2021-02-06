# Data Pipelines with Airflow
### Data Engineer: Matthew Prout

## Project Overview
A music streaming company, Sparkify, has decided that it is time to introduce more automation to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The purpose of this project is to create a high grade data pipelines that is built from reusable tasks, can be monitored, and allows for backfills. The pipeline should also have data quality checks to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Raw Data
JSON logs that record user activity in the application and metadata about the songs that the users listen to.


## Running the code
This project needs to be run from an environment running Apache Airflow, and a Redshift cluster needs to be running in AWS.
The following connections need to be created in Airflow:
1. AWS credentials
2. Redshift database connection

The workflow can be run from the Airflow UI.

## Files:
| File | Purpose |
| - | - |
| `create_tables.sql` | SQL statements to create tables in the Redshift database. |
| `dags/udac_example_dag.py` | Instantiates the DAG, operators in the DAG, and configures the task dependencies. |
| `plugins/helpers/sql_queries.py` | SQL statements for inserting data into the database. |
| `plugins/operators/data_quality.py` | Custom operator to run a data quality check. |
| `plugins/operators/load_dimension.py` | Custom operator to load data into a dimension table. |
| `plugins/operators/load_fact.py` | Custom operator to load data into a fact table. |
| `plugins/operators/stage_redshift.py` | Custom operator to load data from S3 into a staging table in Redshift. |
