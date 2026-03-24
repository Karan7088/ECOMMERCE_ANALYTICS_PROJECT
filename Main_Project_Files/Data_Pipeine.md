# Ecommerce Data Engineering Pipeline

## Overview

This project implements a **production-style ETL pipeline** that extracts data from multiple MySQL databases, performs cleaning and transformations, and stores optimized datasets in **Parquet format**.

The pipeline is designed to handle **large datasets efficiently** using:

* Chunk-based database extraction
* Parallel processing using multiple CPU cores
* Automated data cleaning and imputation
* Memory optimization through datatype downcasting
* Columnar storage using Parquet
* Logging and monitoring
* Automated scheduling

This project simulates a **real-world data engineering workflow**.

---

# Architecture

```
MySQL Databases
     │
     │  (Parallel Extraction)
     ▼
Python ETL Pipeline
     │
     │  (Chunk Processing)
     ▼
Data Cleaning & Transformation
     │
     │  (Memory Optimization)
     ▼
Parquet Data Lake
     │
     ▼
Analytics / BI Tools
```

---

# Project Features

## 1 Parallel Data Extraction

The pipeline uses Python multiprocessing to extract multiple tables simultaneously.

```
ProcessPoolExecutor
```

Benefits:

* Faster extraction
* Better CPU utilization
* Reduced total pipeline runtime

Example parallel tasks:

```
customers_db → customers
ecommerce_db → orders
ecommerce_db → order_items
ecommerce_db → products
ecommerce_db → payments
```

---

# Chunk-Based Data Processing

Large datasets are processed using chunking.

```
pd.read_sql(query, conn, chunksize=500000)
```

Advantages:

* Prevents memory crashes
* Enables processing of very large datasets
* Improves scalability

---

# Parquet Data Storage

Data is stored in **Apache Parquet format** using PyArrow.

Benefits:

* Columnar storage
* Faster reads
* Smaller file sizes
* Ideal for analytics workloads

Compression used:

```
snappy
```

---

# Data Cleaning & Transformation

Each dataset goes through multiple cleaning steps.

Examples:

### Duplicate removal

```
drop_duplicates()
```

### Missing value handling

```
auto_impute()
```

### Outlier handling

```
IQR method
```

### String normalization

```
strip()
lower()
title()
```

### Datetime processing

```
pd.to_datetime()
```

---

# Automatic Data Imputation

The pipeline automatically detects column types and imputes missing values accordingly.

| Data Type | Strategy                        |
| --------- | ------------------------------- |
| Numeric   | Replace with mean               |
| String    | Replace with "Unknown"          |
| Datetime  | Forward fill then backward fill |

---

# Memory Optimization

The pipeline reduces memory usage by:

### Numeric Downcasting

```
pd.to_numeric(downcast="float")
```

### Category Encoding

```
astype("category")
```

### Datetime Downcasting

```
datetime64[s]
```

These optimizations significantly reduce memory footprint.

---

# Data Validation

The pipeline performs validation checks such as:

### Primary key validation

```
assert df["customer_id"].is_unique
```

### Data type checks

### Email validation using regex

### Outlier detection

---

# Logging System

All pipeline events are logged using Python logging.

Log file:

```
pipeline.log
```

Example log entry:

```
2026-03-12 INFO successfully done basic cleaning of group 2 of table orders
```

---

# Parallel Execution Strategy

Instead of sequential execution:

```
orders → order_items → products → payments
```

The pipeline runs them in parallel:

```
orders
     │
order_items
     │
products
     │
payments
```

This reduces pipeline execution time significantly.

---

# Exploratory Data Analysis

The pipeline includes a basic automated EDA function.

Metrics produced:

* Dataset shape
* Null values
* Duplicate rows
* Unique values
* Memory usage
* Possible primary keys

---

# Technologies Used

Programming Language

* Python

Libraries

* pandas
* numpy
* sqlalchemy
* pymysql
* pyarrow
* python-dotenv

Database

* MySQL

Storage Format

* Apache Parquet

Parallel Processing

* concurrent.futures

Scheduling

* Windows Task Scheduler

---

# Environment Variables

Create a `.env` file in the project root.

Example:

```
db_user=your_user
db_pass=your_password
db_host=localhost

CLOUD_DB_USER=cloud_user
CLOUD_DB_PASS=cloud_password
CLOUD_DB_HOST=cloud_host
```

---

# Project Structure

```
Ecommerce_Analytics_Project
│
├── cleaning_and_transformation.py
├── pipeline.log
├── .env
├── customers_db_customers.parquet
├── ecommerce_db_orders.parquet
├── ecommerce_db_order_items.parquet
├── ecommerce_db_products.parquet
├── ecommerce_db_payments.parquet
│
└── README.md
```

---

# Running the Pipeline

Run the pipeline manually:

```
python cleaning_and_transformation.py
```

Pipeline execution order:

```
1 Pre-fetch data from databases
2 Parallel extraction
3 Chunk processing
4 Data cleaning
5 Transformation
6 Parquet generation
```

---

# Automation

The pipeline can be automated using **Windows Task Scheduler**.

Example schedule:

```
Daily at 02:00 AM
```

This enables automated data refresh.

---

# Future Improvements

Possible enhancements:

* Apache Airflow pipeline orchestration
* Cloud storage integration
* Data warehouse loading
* Automated BI dashboard refresh
* Data quality monitoring

---

# Key Learnings

This project demonstrates several core data engineering concepts:

* ETL pipeline design
* Parallel data processing
* Large dataset handling
* Data cleaning pipelines
* Memory optimization
* Columnar data storage
* Pipeline automation

---

# Author

Data Engineering Project

Designed to simulate a real-world **production ETL pipeline**.
