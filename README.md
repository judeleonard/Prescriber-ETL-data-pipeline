# Prescriber-ETL-data-pipeline showing End-to-End implementation using Apache Airflow, pyspark and Apache superset to build BI dashboards

### Project Overview
The dataset for this project contains the information about US Medical prescribers, their cities, years of experience, the cost for each prescribed medicine etc. The data that houses these information came in two format having a relation.The `city dimensions` table in parquet format and the `fact` table that comes in csv format containing the prescribers' information. The goal is to load these data using the spec format into spark and utilizing spark rdd to process and transform by extracting insight off this large dataset about 4GB in size.

### Architecture - Prescriber pipeline
![](https://github.com/judeleonard/Prescriber-ETL-data-pipeline/blob/main/images/spark-architecture.jpeg)

### Technologies and Functions
  - `Pyspark`: Data preprocessing, cleaning & transformation
  - `Python`: data extraction & loading
  - `Amazon s3`: cloud storage as DWH
  - `Azure blob`: Cloud storage as DWH
  - `Postgres`: DWH on premise
  - `Docker`: Containerization 
  - `Apache Airflow`: Orchestration tool and for running Cron jobs
  - `Apache Superset`: BI dashboard

### Spark ETL components/job
  - `City transformation / report`: 
          
      -  filter records by exempting cities which do not have any subscriber assigned
      -  calculate the number of zips in each city
      -  calculate the total transaction in each city
      -  calculate the number distinct prescribers assigned to each city
      
  -  `Prescriber transformation/ report`:
      
      -   apply filter to consider prescribers only from 20 - 50 years of experience
      -   rank the prescribers based on their transaction count for each state
      -   Select top 5 prescribers from each state

  
### Tables in postgres
![](https://github.com/judeleonard/Prescriber-ETL-data-pipeline/blob/main/images/db1.png)
