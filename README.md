
# Sales Data Warehouse & Analytics Project 🚀


## 🏗️ Data Architecture

The project implements a modern **Medallion Architecture**, which logically organizes data into Bronze, Silver, and Gold layers. This approach ensures data quality, scalability, and ease of use for analytics.

![Data Warehouse Layers](docs/Data%20Warehouse%20Layers.png)

* **Bronze Layer (Raw Data)**: Raw data is ingested "as-is" from the source CSV files (ERP and CRM systems) into the MySQL database. This layer serves as the historical archive and single source of truth.
* **Silver Layer (Cleansed & Conformed)**: The data from the Bronze layer undergoes cleaning, validation, and standardization. This is where data quality issues are addressed and data from different sources is integrated.
* **Gold Layer (Business-Ready)**: The final layer houses business-ready, aggregated data modeled into a **Star Schema**. This optimized model is designed for efficient querying, reporting, and business intelligence.

---

## 📊 Project Overview

This project is a complete data lifecycle demonstration, encompassing:

* **ETL Pipelines**: Building robust ETL (Extract, Transform, Load) processes using SQL to move data from source systems into the data warehouse.
* **Data Modeling**: Designing and implementing a star schema with clear fact and dimension tables, optimized for analytical queries.
* **Analytics & Reporting**: Writing complex SQL queries to derive key business insights on customer behavior, product performance, and sales trends.
* **Data Integration**: Merging data from disparate ERP and CRM systems into a unified, coherent model.

![Data Integration](docs/Data%20integration.jpg)

---

## 🛠️ Tools & Technologies

* **Database**: MySQL
* **Data Modeling & Architecture Design**: Draw.io
* **Version Control**: Git & GitHub
* **IDE**: An SQL client of your choice (e.g., MySQL Workbench, DBeaver)

---

## 📈 Data Flow & Schema

The data flows from raw files through the Bronze and Silver layers, ultimately landing in the Gold layer, which is structured as a star schema. This design places the core business metrics (**Fact Table**) at the center, linked to descriptive attributes (**Dimension Tables**).

### Data Flow Diagram
![Data Flow](docs/Data%20Flow.jpg)

### Gold Layer: Star Schema
This schema simplifies complex queries and allows for fast aggregations, making it ideal for analytics.
![Star Schema](docs/Star.jpg)

---

## 📂 Repository Structure

```
.
├── datasets/             # Raw source data (ERP and CRM CSV files)
├── docs/                 # Project documentation and diagrams
│   ├── Data Architecture approaches.png
│   ├── Data Flow.jpg
│   ├── Data integration.jpg
│   ├── Data Warehouse Layers.png
│   └── Star.jpg
├── scripts/              # SQL scripts for the entire ETL process
│   ├── 1_bronze/         # Scripts for creating schemas and loading raw data
│   ├── 2_silver/         # Scripts for data cleaning and transformation
│   └── 3_gold/           # Scripts for creating the final star schema and BI queries
└── README.md             # You are here!
```

---

## 🚀 How to Run This Project

1.  **Set up MySQL**: Ensure you have a running instance of MySQL.
2.  **Create Database**: Create a new database for this project.
3.  **Run Scripts**: Execute the SQL scripts in the following order:
    1.  `scripts/1_bronze/`: Load the raw data from the `datasets` folder.
    2.  `scripts/2_silver/`: Run the transformation scripts to clean and integrate the data.
    3.  `scripts/3_gold/`: Build the final star schema and run the analytical queries to see the insights.

---

## ✅ Key Skills Demonstrated

This project showcases my expertise in:

* **SQL Development**: Writing efficient, scalable, and complex SQL queries for data transformation and analysis.
* **Data Architecture**: Designing and implementing a modern data warehouse using the Medallion Architecture.
* **ETL Pipeline Development**: Building data pipelines to handle extraction, transformation, and loading.
* **Data Modeling**: Creating logical and physical data models (Star Schema) optimized for analytics.
* **Data Analysis**: Translating business requirements into technical queries to deliver actionable insights.

Thank you for reviewing my project! Feel free to reach out if you have any questions.
