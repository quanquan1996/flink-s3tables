# Flink S3Tables Integration Demo

This is the first open-source demonstration project showcasing how to integrate Apache Flink 1.20.1 with AWS S3Tables and Apache Iceberg.this project fills an important gap in the community resources and provides a foundation for developers to build upon when implementing similar solutions.

## Project Overview

This groundbreaking demo is the first publicly available example that demonstrates how to connect to AWS Glue Catalog through Flink SQL interface and perform basic operations on Iceberg tables stored in S3Tables, such as querying tables, listing databases and tables, and inserting data.

As the pioneer open-source implementation of S3Tables with Flink integration, this project serves as a valuable reference for developers looking to leverage these technologies together.

## Key Features

• Connect to AWS Glue Catalog
• List databases and tables
• Query table structures and data
• Perform data insertion operations
• **First public demonstration** of S3Tables with Flink integration

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


## Significance

As the first open-source demonstration of Flink with S3Tables integration, this project fills an important gap in the community resources and provides a foundation for developers to build upon when implementing similar solutions.
