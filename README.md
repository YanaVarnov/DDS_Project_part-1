# 📘 Project 1 — Distributed Data Processing with PySpark (DDM)
## Overview

This project focuses on large-scale data processing and analytical querying using PySpark in a distributed environment.
The goal was to detect anomalous and corrupted TV program data, analyze viewing behavior across regions and demographics, and derive actionable insights using scalable data transformations.

The project was implemented entirely on Databricks using Apache Spark.

# Data

* The dataset consists of multiple large-scale tables, including:

* Program metadata (titles, genres, duration)

* TV viewing events

* Household demographic attributes

* Device and regional (DMA) information

The data is distributed across multiple schemas and contains missing values, duplicates, and categorical attributes requiring preprocessing.

# Tasks & Methodology
# Part 1 — Anomaly Detection

Performed data cleaning and preprocessing using PySpark DataFrames

Engineered helper columns to optimize repeated computations

Identified anomalous program airings based on compound logical conditions

Flagged malicious program titles based on aggregated anomaly ratios

# Part 2 — Analytical Queries

Executed distributed analytical queries to:

* Identify popular genres, programs, and regions

* Analyze viewing behavior in households with children

* Computed region-level metrics using aggregation and normalization

* Designed logic to prioritize regions based on derived wealth scores

# Technologies Used

Python

PySpark / Spark SQL

Databricks

Distributed joins, aggregations, and filtering

# Key Takeaways

Experience working with multi-table distributed datasets

Efficient use of PySpark transformations for complex logic

Design of scalable analytical workflows on large data
