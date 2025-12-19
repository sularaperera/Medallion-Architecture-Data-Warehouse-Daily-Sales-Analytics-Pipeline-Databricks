<img src="https://github.com/sularaperera/Medallion-Architecture-Data-Warehouse-Daily-Sales-Analytics-Pipeline-Databricks/blob/main/Cover_Image.jpg"></img>

# Medallion-Architecture-Data-Warehouse-Daily-Sales-Analytics-Pipeline


### Project Overview
This project implements a medallion architecture data warehouse on Databricks, transforming raw daily sales CSV files into a structured, analytics-ready data platform. The solution follows industry best practices for data engineering, utilizing Delta Lake tables and a multi-layered data processing approach to ensure data quality, traceability, and scalability.

### Architecture Design

#### Medallion Architecture Layers
The project implements a **four-layer medallion architecture**, each serving a specific purpose in the data transformation journey:

- 00_source  → Raw ingestion layer (landing zone)
- 01_bronze  → Raw data preservation with metadata
- 02_silver  → Cleaned, validated, conformed data
- 03_gold    → Business-ready aggregated metrics
