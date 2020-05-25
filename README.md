# Data Engineering Nanodegree

Projects and resources developed in the [DEND Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) from Udacity.

## Project 1: [Relational Databases - Data Modeling with PostgreSQL](https://github.com/dvu4/udacity-data-engineering/tree/master/data-modeling/project-1-postgres).

[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)

Develope a relational database using PostgreSQL to model user activity data for a music streaming app. Skills include:
* Created a relational database using PostgreSQL
* Developed a Star Schema database using optimized definitions of Fact and Dimension tables. Normalization of tables.
* Built out an ETL pipeline to optimize queries in order to understand what songs users listen to.

Technologies used: Python, PostgreSql, Star Schema, ETL pipelines, Normalization


## Project 2: [NoSQL Database - Data Modeling with Apache Cassandra](https://github.com/dvu4/udacity-data-engineering/tree/master/data-modeling/project-2-apache-cassandra).

[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)



Develop NoSQL database with Cassandra and build an ETL pipeline using Python based on the original schema outlined in project one. We want to get some answers around the queries :
* Get details of a song that was herad on the music app history during a particular session.
* Get songs played by a user during particular session on music app.
* Get all users from the music app history who listened to a particular song.

Technologies used: Python, Apache Cassandra, Denormalization



## Project 3: [Data Warehouse - Amazon Redshift](https://github.com/dvu4/udacity-data-engineering/tree/master/data-warehouse/project-3-data-warehouse-aws).

[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)

Apply the Data Warehouse architectures we learnt and build a Data Warehouse on AWS Redshift. 

* Build an ETL pipeline to extract and transform data stored in JSON format from S3 buckets into staging tables.
* Move the data to Warehouse hosted on Amazon Redshift Cluster.
* Develope the optimized queries required by the data analytics team

Technologies used: Python, Amazon Redshift, AWS CLI, Amazon SDK, SQL, PostgreSQL



## Project 4: [Data Lake - Spark](https://github.com/dvu4/udacity-data-engineering/tree/master/data-lake/project-4-data-lake-with-spark).

[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)

Build a Data Lake on AWS cloud using Spark and AWS EMR cluster. The data lake will serve as a Single Source of Truth (SSOT) for the Analytics Platform. Spark jobs are created to scale up ELT pipeline that moves data from landing zone on S3 (data warehouse) and transform and stores data in processed zone on S3 (data lake).

* Create an EMR Hadoop Cluster
* Further develop the ETL Pipeline copying datasets from S3 buckets, data processing using Spark and writing to S3 buckets using efficient partitioning and parquet formatting.
* Fast-tracking the data lake buildout using (serverless) AWS Lambda and cataloging tables with AWS Glue Crawler.

Technologies used: Python, Spark, AWS S3, EMR, Athena, Amazon Glue, Parquet.
