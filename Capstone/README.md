# Udacity Data Engineering Nanodegree Capstone Project
## Data Warehouse for Insights into International Tourism
### Matthew Prout


# Overview and Purpose

This capstone project uses the skills I have learned in the data engineering nanodegree to process a data set for a particular use case.

The purpose of my project is to create a data warehouse that will allow analysts to gain insights into international travel.

In this file, I will discuss the steps of the project, the design decisions I made (data model and tools), and other considerations regarding scalability.


# Outline of Steps in Project

The following steps were taken in the project:

1. First, the scope of the project was determined and the data was gathered. In this case, I chose to use the Udacity provided data set, and enriched it with one additional data source.
2. Second, I explored and cleaned the data. The data was examined to understand the types, missing data, and ranges. Based off of the exploratory data analysis, I cleaned the data and wrote each data set to a Parquet file.  
3. Third, I defined the data model. This model was implemented as a data warehouse.
4. Fourth, the data warehouse was created, data was loaded from the Parquet files, and the fact and dimension tables were updated.


# Files

| File | Description |
| ------ | ------ |
| `README.md` | The write-up for this project. |
| `CreateCluster.ipynb` | A notebook which creates a Redshift cluster. |
| `dwh.cfg` | The configurations file for the project. |
| `Capstone Project - International Tourism.ipynb` | The notebook used to explore and cleaned the data sets, and then run the steps of the ETL process. |
| `sql_queries.py` | The queries used to create the database and to perform the ETL. |
| `create_tables.py` | The script to create the tables in the database. |
| `etl.py` | The script to update the data into database. |
| `quality_checks.py` | Checks run after the ETL process to verify the ingestion. |
| `data/BrentOilPrices.csv` | The price of Brent oil as a time series. |
| `data/CountryCodes.csv` | The country codes that map to country names from the I94_SAS_Labels_Descriptons.SAS file. |
| `data/GlobalLandTemperaturesByState.csv` | Temperatures for states in different countries, as a time series. |
| `data/Immigration_data_sample.csv` | A sample of the I94 immigration data. |
| `data/PortCodes.csv` | The port codes that map to port names from the I94_SAS_Labels_Descriptons.SAS file. |
| `data/StateCodes.csv` | A map of US state abbreviations to state names. |
| `data/us-cities-demographics.csv` | Demographic information for US cities. |
| `images` | A directory containing images used by the README and notebook files. |
| `sas_data` | I94 immigration data stored using the Parquet format. |


# Running the Project

1. The user needs to add their AWS credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) to `dwh.cfg`.
2. Update S3A_BUCKET and S3_BUCKET in `dwh.cfg` with the name of an S3 bucket to write to.  The *s3a* protocol is needed to write to Parquet files from Spark.  The *s3* protocol is needed to load Parquet files into Redshift.
3. Create the Redshift cluster by running `CreateCluster.ipynb` through step 4.
4. Update DB_ENDPOINT and ARN in `dwh.cfg` with the values from the Redshift cluster displayed in `CreateCluster.ipynb`.
5. Run `Capstone Project-International Tourism.ipynb` through section 2 'Explore and Assess the Data' to clean, transform, and write the data to S3.
6. Run `Capstone Project-International Tourism.ipynb` section 4 'Run Pipelines to Model the Data' to create the database, load the database, and perform quality checks.
7. Run any queries on Redshift using the query editor in AWS.
8. Clean up the Redshift cluster by running `CreateCluster.ipynb` step 5 onwards.


# Choice of Data Model

The data model that was chosen is a star schema.  The star schema was chosen as it is an efficient design for performing OLAP queries (such as joins and aggregations), which is the purpose of this particular data warehouse.


# Choice of Tools

The following tools were chosen for the project:
* S3:  Amazon Simple Storage Service is the best storage option when using AWS, as it is integrated with the other services.
* Apache Spark:  Spark is an efficient tool for cleaning and transforming large data sets, like the type that this project deals with.  Spark can run on a cluster that can be scaled out to support larger data sets.
* Parquet file format:  Parquet supports compression, therefore saving storage space, and making the file transfer faster from the cleaned data into Redshift.
* Amazon Redshift: Redshift is a data warehouse product, which supports this use case for gaining insights into international travel using OLAP queries.

A production deployment of the project should include the following changes:
* The ingestion data would be staged on S3 or another storage server.
* Spark would run on its own EMR cluster.
* Apache Airflow would run on a dedicated EC2 instance to orchestrate the workflow.


# How Often the Data Should be Updated

The lowest level of granularity of the analysis is at the month level, so the data should be updated at least once a month.


# Other Scenarios

## The data was increased by 100x

If the data were to increase 100x, then the pipeline may need to scale.  Measurements in each part of the pipeline would need to be taken to determine the critical path.  Also, requirements would need to be determined on how often the pipeline needs to be run.  If the pipeline needs to run end-to-end every 24 hours, then different portions of the pipeline could be scaled to achieve E2E execution in 24 hours.  
* S3 supports storage size scalability, so we can continue using S3 when scaling up.
* If the data transfer between the Elastic MapReduce (EMR) cluster and S3 is a bottleneck for time, the bandwidth would need to be increased to keep up with the increase in size.  This can be done by both keeping the S3 bucket(s) in the same region as the EMR cluster, and by spreading out the storage of files among separate buckets, or even using byte-range fetches to copy large files with multiple, simultaneous, connections.
* To scale Spark, it would be necessary to add more nodes to the EMR cluster to enable it to scale linearly. 
* The COPY transfer from S3 to Redshift scales with the number of Redshift nodes.  Redshift allows data to be loaded in parallel, so scalability is built-in.
* To scale Redshift, additional nodes can be added to the cluster.


## The data populates a dashboard that must be updated on a daily basis by 7am every day

In order to run the pipeline on a daily basis, it should be automated using a tool like Airflow.  
Because the requirement is to populate the dashboard on a daily basis, this means the data warehouse needs to be updated each day.  In order for the data warehouse to be updated each day, the pipeline must be able to be run within 24 hours.  Therefore the pipeline may have to be scaled in order to accommodate this requirement, per the previous question.  


## The database needed to be accessed by 100+ people

Query speed on Redshift is proportional to the number of nodes in the cluster.  AWS provides the Concurrency Scaling feature that enables the cluster to automatically resize the number of nodes to maintain fast performance for read queries.  So if larger loads are intermittent, then you can provision a small cluster and rely on Concurrency Scaling.  Otherwise if the load is sustained at or above 100 queries, then you can provision a larger Redshift cluster.  

There are other techniques to keeping Redshift performant:
* Queries can be re-written to be more efficient
* The pipeline can be run when people are not querying the database, so loading data does not affect query times.
* The database can be optimized by using distribution keys and sort keys.

# Example Queries

1. Create a time series of international visitor counts and average temperatures in NY for each month in 2016.  This series can give an indication of a correlation between weather and tourism.

Query:

`SELECT visit_count_table.month, visitcount, monthly_average_temp
FROM
(
    SELECT datetime.month, count(*) AS visitcount
    FROM visit
    INNER JOIN datetime on visit.arrival_date = datetime.event
    WHERE (visit.destination_state = 'NY') AND (datetime.year = 2016)
    GROUP BY visit.destination_state, datetime.month
    ORDER BY datetime.month
) visit_count_table
INNER JOIN
(
    SELECT month, monthly_average_temp
    FROM tempbystate
    WHERE year = 2016
    ORDER BY month
) temp_table
ON visit_count_table.month = temp_table.month
ORDER BY temp_table.month;`

2. Create a time series of international visitor counts to the US and oil prices each month in 2016.  This series can give an indication of a correlation between oil prices and tourism.

Query:

`SELECT visit_count_table.year, visit_count_table.month, visitcount, monthly_average_price
FROM
(
    SELECT year, month, count(*) AS visitcount
    FROM visit
    INNER JOIN datetime ON visit.arrival_date = datetime.event
    WHERE year BETWEEN 2005 AND 2016
    GROUP BY year, month
) visit_count_table
INNER JOIN
(
    SELECT year, month, monthly_average_price
    FROM oilprice
    WHERE year BETWEEN 2005 AND 2016
) oil_price_table
ON visit_count_table.year = oil_price_table.year AND visit_count_table.month = oil_price_table.month
ORDER BY visit_count_table.year, visit_count_table.month;`
