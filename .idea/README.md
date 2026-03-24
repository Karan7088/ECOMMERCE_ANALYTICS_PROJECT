# 🚀 E-Commerce Analytics Data Engineering & BI Project

## 📌 Project Overview
This project is an **end-to-end data engineering + analytics solution** built to process large-scale e-commerce data and generate actionable business insights.

It covers:
- ⚙️ Data Extraction from multiple MySQL databases  
- 🧹 Automated Data Cleaning & Transformation using Python  
- ⚡ Parallel Processing for high-performance data fetching  
- 📦 Efficient storage using Parquet format  
- 📊 Interactive BI Dashboard for business insights  

The pipeline processes **~7.5 million+ rows of data** to simulate real-world production-scale workloads.

---

## 🏗️ Architecture

MySQL DBs → Python ETL Pipeline → Parquet Files → BI Dashboard

---

## 🔹 Data Sources
- `customers_db`
- `ecommerce_db`
  - orders
  - order_items
  - products
  - payments

---

## ⚙️ Tech Stack

- **Python** (Core Processing)
- **Pandas / NumPy** (Data Transformation)
- **SQLAlchemy + MySQL** (Data Extraction)
- **PyArrow (Parquet)** (Efficient Storage)
- **Concurrent Futures** (Parallel Processing)
- **Logging + dotenv** (Production Practices)
- **Power BI / BI Tool** (Visualization)

---

## ⚡ Key Features

### 🔹 Parallel Data Fetching
- Uses `ProcessPoolExecutor`
- Fetches multiple tables simultaneously
- Optimized for performance without overloading DB

### 🔹 Chunk-Based Processing
- Reads large datasets in chunks (`500K rows`)
- Prevents memory overflow
- Scales to millions of records

### 🔹 Automated Data Cleaning Engine
- Handles missing values
- Downcasts datatypes for memory optimization
- Standardizes strings & categories
- Fixes datetime inconsistencies

### 🔹 Table-Specific Transformations

**Customers**
- Email validation
- Duplicate removal
- Multi-index creation

**Orders**
- Status normalization
- Date feature extraction (year, month)

**Order Items**
- Outlier handling using IQR

**Payments**
- Digital payment classification
- Amount outlier clipping

**Products**
- Category standardization
- Derived metric: `total_price`

---

### 🔹 Data Quality Checks
- Primary key validation using assertions
- Duplicate detection
- Null analysis
- Memory optimization

---

### 🔹 Optimized Storage
- Stored in **Parquet format**
- Snappy compression
- Faster read/write

---

## 📊 BI Dashboard Insights

### 📈 Sales & Orders Analysis
- Total Sales: **₹5.12B+**
- Total Orders: **~1M**
- AOV: **₹924**
- Monthly trends analysis

### 🛍️ Product Insights
- Top-selling products
- Category performance
- Quantity distribution

### 👥 Customer Insights
- Total Customers: **500K+**
- New vs Repeat Customers
- Churn Rate (~99%)
- Top customers by revenue

### 💳 Payment Insights
- Payment method distribution
- Digital vs Non-digital usage

---

## 📁 Project Structure

📦 Ecommerce_Analytics_Project  
┣ 📂 data/  
┣ 📂 parquet_files/  
┣ 📂 scripts/  
┃ ┗ 📜 pipeline.py  
┣ 📂 logs/  
┣ 📂 dashboard/  
┃ ┗ 📊 BI Dashboard File  
┣ 📜 .env  
┣ 📜 requirements.txt  
┗ 📜 README.md  

---

## ▶️ How to Run

### 1. Clone Repo
```bash
git clone <your-repo-url>
cd Ecommerce_Analytics_Project
