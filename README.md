# Aero Data Engineering Project

## Overview
This project implements an end-to-end data pipeline using the Bronze-Silver-Gold architecture in Databricks with PySpark.

## Architecture
- **Bronze Layer**: Raw data ingestion from source systems
- **Silver Layer**: Data cleaning, filtering, and transformation
- **Gold Layer**: Business-level aggregations and analytics tables

## Technologies Used
- PySpark
- Databricks
- Delta Lake
- GitHub

## Pipeline Flow
1. Ingest raw data into Bronze layer
2. Transform and clean data in Silver layer
3. Build fact and dimension tables in Gold layer

## Use Case
This pipeline simulates processing of raw data into analytics-ready datasets for reporting and business insights.
