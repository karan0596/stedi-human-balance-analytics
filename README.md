# STEDI Human Balance Analytics - Data Lakehouse Project

## Project Overview

The STEDI Human Balance Analytics project simulates a real-world data engineering pipeline for a IoT system designed to support human balance training and motion detection. In this project, I act as a data engineer for the STEDI team, responsible for building a scalable data lakehouse solution on AWS to process sensor data and prepare it for machine learning model training.

The STEDI team has developed a hardware device called the STEDI Step Trainer, which helps users perform balance exercises while capturing motion data through built-in sensors. The device continuously records distance-based motion data to detect steps. In addition, a companion mobile application collects accelerometer data from smartphones, capturing motion in the X, Y, and Z axes. Together, these data sources provide rich time-series information that can be used to train a machine learning model capable of detecting steps in real time.

The system is intended for early adopters who are using both the Step Trainer device and the mobile application. These users generate large volumes of sensor data as they perform daily balance training exercises.

A critical aspect of this project is data privacy and governance. Only a subset of users—those who have explicitly agreed to share their data for research purposes—are included in the analytics pipeline. Personally Identifiable Information (PII) such as names, email addresses, phone numbers, and other sensitive attributes must be filtered out before data is used for analytics or machine learning.

The primary goal of this project is to design and implement an end-to-end AWS-based data lakehouse architecture using Amazon S3, AWS Glue, and Amazon Athena. Raw sensor data is ingested into landing zones, cleaned and filtered into trusted zones, and finally transformed into curated datasets suitable for analytical and machine learning workloads.

By the end of the pipeline, only validated and consented customer data from both the Step Trainer and accelerometer sources are combined to produce high-quality curated datasets that data scientists can use to train and improve real-time step detection models.


## Technologies Used
- AWS Glue Studio
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- Apache Spark

## Data Flow
Data flows from the Landing Zone (raw ingested S3 data) to the Trusted Zone (filtered and validated records) and finally to the Curated Zone (anonymized, analytics and machine learning–ready datasets).  

### 📥 Landing Zone (Raw Data)

- Raw data is ingested and stored in Amazon S3 from multiple sources:
  - Customer website data
  - Accelerometer data from mobile application
  - Step trainer IoT sensor data

- Data is stored in semi-structured format (JSON/CSV) inside S3 buckets:
  - customer_landing
  - accelerometer_landing
  - step_trainer_landing
  
- AWS Glue Data Catalog tables are created for each dataset to enable querying and exploration

### 🧹 Trusted Zone (Filtered Data)

- AWS Glue ETL jobs are created to filter raw data from the Landing Zone.
  - Customer data is filtered to include only users who agreed to share data for research.
  - Accelerometer data is filtered to include only readings from consenting customers.

- Step trainer data is filtered to include only:
  - customers with accelerometer data
  - customers who have given research consent

- All datasets in this zone are cleaned and structured for downstream processing and analytics.

### 🧠 Curated Zone (Aggregated, ML-Ready & Anonymized Data)

- Final curated data is created by joining Step Trainer and Accelerometer datasets:
  - Step Trainer readings are matched with Accelerometer readings on the same timestamp
  - Only customers who have agreed to share their data are included

- Data is further transformed to ensure privacy compliance (GDPR-aligned):
  - The `user` field is removed to prevent direct identification of customers.
