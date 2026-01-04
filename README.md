# 🚚 Predictive Supply Chain Control

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![SQLite](https://img.shields.io/badge/SQLite-Data%20Warehouse-003B57?style=for-the-badge&logo=sqlite&logoColor=white)

## 📌 Project Overview
This project implements an end-to-end **Business Intelligence & Analytics** solution for supply chain management. It ingests raw logs, transforms them into a **Star Schema Data Warehouse**, and visualizes shipping risks via an interactive **Power BI Dashboard**.

The analysis revealed a critical **process failure in "First Class" shipping** (100% late rate due to unrealistic 1-day promises), leading to a recommendation that could save **$14k annually** by optimizing Service Level Agreements (SLAs).

## 📂 Data Source
* **Dataset:** [DataCo Smart Supply Chain for Big Data Analysis](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis)
* **Volume:** ~180k Records

## 🏗️ Architecture
1. **ETL Pipeline (`etl_engine.py`):**
   - **Extract:** Reads raw CSV (handles encoding errors).
   - **Transform:** Cleanses data and normalizes it into 3 Dimensions (`Customer`, `Product`, `Location`) and 1 Fact Table (`Orders`).
   - **Load:** Ingests data into a SQLite database and exports refined "Star Schema" CSVs for BI ingestion.
2. **Predictive Modeling (`generate_powerbi_data.py`):**
   - Trains a **Random Forest Classifier** to predict late delivery risk (0/1).
   - Generates a "Confidence Score" for every shipment.
3. **Control Tower (Power BI):**
   - **Decomposition Tree:** Identifying root causes of delays (Region vs. Shipping Mode).
   - **Gap Analysis:** Visualizing "Promised Days" vs. "Actual Days" to expose operational failures.

## 📊 Dashboard Key Features
* **Root Cause Analysis:** Interactive drill-down into the 35% global late risk.
* **Operational Insight:** Identified that **First Class** shipping fails 99% of the time because the actual transit time (2 days) exceeds the promised time (1 day).
* **Adjusted Risk Logic:** implemented a DAX-based "Grace Period" scenario that re-evaluates risk with a +1 day buffer.

Author: Aditya Rudre
