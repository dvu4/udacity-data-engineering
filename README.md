# Data Engineering Nanodegree

Projects and resources developed in the [DEND Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) from Udacity.

## Project 1: [Relational Databases - Data Modeling with PostgreSQL](https://github.com/dvu4/udacity-data-engineering/tree/master/data-modeling/project-1-postgres).

[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)

Developed a relational database using PostgreSQL to model user activity data for a music streaming app. Skills include:
* Created a relational database using PostgreSQL
* Developed a Star Schema database using optimized definitions of Fact and Dimension tables. Normalization of tables.
* Built out an ETL pipeline to optimize queries in order to understand what songs users listen to.

Technologies used: Python, PostgreSql, Star Schema, ETL pipelines, Normalization


## Project 2: [NoSQL Database - Data Modeling with Apache Cassandra](https://github.com/dvu4/udacity-data-engineering/tree/master/data-modeling/project-2-apache-cassandra).

[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)



Developed NoSQL database with Cassandra and build an ETL pipeline using Python based on the original schema outlined in project one. We want to get some answers around the queries :
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
