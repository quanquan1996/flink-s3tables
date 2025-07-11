# Flink S3Tables Integration Demo

This is a simple demonstration project showcasing how to integrate Apache Flink 1.20.1 with AWS S3Tables and Apache Iceberg.

## Project Overview

This project demonstrates how to connect to AWS Glue Catalog through Flink SQL interface and perform basic operations on Iceberg tables stored in S3, such as querying tables, listing databases and tables, and inserting data.

## Key Features

• Connect to AWS Glue Catalog
• List databases and tables
• Query table structures and data
• Perform data insertion operations

## Technology Stack

• Apache Flink 1.20.1
• Apache Iceberg 1.8.1
• AWS S3Tables
• AWS Glue Catalog
• Java 11

## Usage

Build and run the project, specifying AWS region, account ID, bucket name, database name, and table name through command-line parameters.

java -cp target/flink-demo-1.0-SNAPSHOT.jar org.example.IcebergGlueIntegrationTest \
--region us-west-2 \
--account 051826712157 \
--bucket testtable \
--database testdb \
--table test_table
