<img src="https://github.com/sularaperera/Medallion-Architecture-Data-Warehouse-Daily-Sales-Analytics-Pipeline-Databricks/blob/main/github_cover.jpg"></img>

# Medallion-Architecture-Data-Warehouse-Daily-Sales-Analytics-Pipeline


### Project Overview
This project implements a medallion architecture data warehouse on Databricks, transforming raw daily sales CSV files into a structured, analytics-ready data platform. The solution follows industry best practices for data engineering, utilizing Delta Lake tables and a multi-layered data processing approach to ensure data quality, traceability, and scalability.

### Architecture Design

#### Medallion Architecture Layers
The project implements a **four-layer medallion architecture**, each serving a specific purpose in the data transformation journey:

- **00_source**  → Raw ingestion layer (landing zone)
- **01_bronze**  → Raw data preservation with metadata
- **02_silver**  → Cleaned, validated, conformed data
- **03_gold**    → Business-ready aggregated metrics

### Infrastructure Setup
#### Databricks Environment:

- **Platform**: Databricks Free Edition (Community Edition)
- **Workspace**: ppl_data_warehouse
- **Catalog**: ppl_data_warehouse (Unity Catalog structure)
- **Storage Format**: Delta Lake (ACID-compliant)

#### Organizational Structure:

- **Notebooks**: source, bronze, silver (modular ETL pipeline)
- **Schemas**: Four dedicated schemas matching architecture layers
- **Volume:** `customer_extract_data` in `00_source` for CSV file storage

## Source Data Specification

### Data Source
- **Format:** CSV files (comma-separated values)
- **Frequency:** Daily extracts
- **Location:** Volume storage at `/Volumes/project_2_ppl_data_warehouse/00_source/customer_extract_data/`
- **Schema:** 36 columns covering customer, order, product, and fulfillment dimensions

### Data Schema (36 Columns)

#### Customer Dimension:
- `Customer Id` - Unique customer identifier
- `Account Name` - Customer business name
- `Account Contact` - Primary contact person
- `Account Contact Email` - Contact email address
- `Billing Contact` - Billing department contact
- `Billing Email` - Billing email address

#### Location Dimension:
- `Site Name` - Delivery site identifier
- `Street` - Street address
- `Building` - Building number/name
- `Suburb` - Suburb/neighborhood
- `City` - City name

#### Order Header:
- `Order No` - Unique order identifier
- `PO Number` - Purchase order reference
- `Delivery Date` - Scheduled delivery timestamp
- `Status Type` - Order status (pending, completed, etc.)
- `Delivered Date` - Actual delivery timestamp

#### Order Metrics:
- `Ordered Total` - Total order value
- `Packed Total` - Total packed value
- `Units` - Number of units

#### Product Details:
- `Sku` - Stock keeping unit (product code)
- `Product Title` - Product description
- `Product Type` - Product category
- `Ordered` - Quantity ordered
- `Packed` - Quantity packed
- `Buy Price` - Cost price
- `Normal Price` - Retail price
- `Ordered LineTotal` - Line item ordered value
- `Packed LineTotal` - Line item packed value

#### Fulfillment Details:
- `Truck No` - Delivery truck identifier
- `Bin Number` - Warehouse bin location
- `Driver Returned` - Driver return quantity
- `Credit Note Returned` - Credit note return quantity
- `Temperature` - Storage temperature requirement
- `Frozen Temperature` - Frozen storage indicator
- `Fulfilment System` - Fulfillment system identifier
- `Provider` - Logistics provider
