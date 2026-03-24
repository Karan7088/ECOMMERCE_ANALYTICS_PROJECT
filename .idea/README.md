🚀 E-Commerce Analytics Data Engineering & BI Project
📌 Project Overview

This project is an end-to-end data engineering + analytics solution built to process large-scale e-commerce data and generate actionable business insights.

It covers:

⚙️ Data Extraction from multiple MySQL databases
🧹 Automated Data Cleaning & Transformation using Python
⚡ Parallel Processing for high-performance data fetching
📦 Efficient storage using Parquet format
📊 Interactive BI Dashboard for business insights

The pipeline processes ~7.5 million+ rows of data to simulate real-world production-scale workloads.

🏗️ Architecture
MySQL DBs → Python ETL Pipeline → Parquet Files → BI Dashboard
🔹 Data Sources
customers_db
ecommerce_db
orders
order_items
products
payments
⚙️ Tech Stack
Python (Core Processing)
Pandas / NumPy (Data Transformation)
SQLAlchemy + MySQL (Data Extraction)
PyArrow (Parquet) (Efficient Storage)
Concurrent Futures (Parallel Processing)
Logging + dotenv (Production Practices)
Power BI / BI Tool (Visualization)
⚡ Key Features
🔹 1. Parallel Data Fetching
Uses ProcessPoolExecutor
Fetches multiple tables simultaneously
Optimized for performance without overloading DB
🔹 2. Chunk-Based Processing
Reads large datasets in chunks (500K rows)
Prevents memory overflow
Scales to millions of records
🔹 3. Automated Data Cleaning Engine

Reusable function:

Handles missing values
Downcasts datatypes for memory optimization
Standardizes strings & categories
Fixes datetime inconsistencies
🔹 4. Table-Specific Transformations

Each dataset is cleaned with domain logic:

Customers
Email validation
Duplicate removal
Multi-index creation
Orders
Status normalization
Date feature extraction (year, month)
Order Items
Outlier handling using IQR
Payments
Digital payment classification
Amount outlier clipping
Products
Category standardization
Derived metric: total_price
🔹 5. Data Quality Checks
Primary key validation using assertions
Duplicate detection
Null analysis
Memory usage optimization
🔹 6. Optimized Storage
Data stored in Parquet format
Snappy compression
Faster read/write for analytics
📊 BI Dashboard Insights

The dashboard provides deep business insights across:

📈 Sales & Orders Analysis
Total Sales: ₹5.12B+
Total Orders: ~1M
AOV (Average Order Value): ₹924
Monthly sales & order trends
🛍️ Product Insights
Top-selling products by revenue
Category-wise performance
Quantity distribution across categories
👥 Customer Insights
Total Customers: 500K+
New vs Repeat Customers
Churn Rate Analysis (~99%)
Top customers by revenue
💳 Payment Insights
Payment method distribution
Digital vs Non-digital trends
📁 Project Structure
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
▶️ How to Run
1️⃣ Clone Repo
git clone <your-repo-url>
cd Ecommerce_Analytics_Project
2️⃣ Setup Environment
pip install -r requirements.txt
3️⃣ Configure .env
db_user=your_user
db_pass=your_pass
db_host=your_host

CLOUD_DB_USER=your_user
CLOUD_DB_PASS=your_pass
CLOUD_DB_HOST=your_host
4️⃣ Run Pipeline
python main.py
📌 Key Learnings
Handling large-scale data pipelines
Designing modular ETL systems
Using parallel processing in real-world scenarios
Implementing data quality checks
Building business-ready dashboards
🎯 Business Impact

This project helps:

Identify top-performing products
Understand customer behavior
Track revenue trends
Optimize sales strategies
🔥 Highlight

Built a scalable pipeline processing 7.5M+ rows with parallel execution + optimized storage, and converted raw data into actionable business insights via BI dashboard.

🤝 Connect With Me

If you liked this project, feel free to connect or give feedback 🚀
