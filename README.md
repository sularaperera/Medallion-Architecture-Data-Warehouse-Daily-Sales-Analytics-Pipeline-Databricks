<img src="https://github.com/sularaperera/Medallion-Architecture-Data-Warehouse-Daily-Sales-Analytics-Pipeline-Databricks/blob/main/github_cover.png"></img>

# Data Warehouse ETL Project: Medallion Architecture with SCD Implementation

## 🎯 Project Overview

This project demonstrates a comprehensive end-to-end data warehouse solution built on **Databricks**, implementing industry-standard **Medallion Architecture** and **Star Schema** design patterns. The project showcases advanced data engineering concepts including incremental data loading, slowly changing dimensions (SCD Type 1 and Type 2), and dimensional modeling best practices.

**Key Highlights:**
- Full ETL pipeline implementation using Databricks
- Medallion Architecture (Bronze → Silver → Gold layers)
- Star Schema dimensional modeling
- Incremental data loading with MERGE operations
- SCD Type 1 (no history retention) and Type 2 (full history tracking)
- Surrogate key implementation for dimension tables
- Fact and dimension table creation with proper relationships

---

## 🏗️ Architecture Overview

### Medallion Architecture Layers

The project follows the **Medallion Architecture**, a best-practice design pattern for data lakes that organizes data into three progressive layers:

```
Source Layer → Bronze Layer → Silver Layer → Gold Layer
```


#### **1. Source Layer (Landing Zone)**
- **Purpose**: Raw data ingestion point
- **Schema**: `project_1_data_modeling.default`
- **Function**: Stores the original, unprocessed data from source systems
- **Data Format**: Flat table structure with 13 columns including order details, customer information, product data, and timestamps

#### **2. Bronze Layer (Staging/Raw)**
- **Purpose**: Incremental data ingestion with minimal transformation
- **Schema**: `project_1_data_modeling.bronze`
- **Function**: Implements **transient staging** approach for efficient incremental loading
- **Loading Strategy**: Delta-based incremental loading using `order_date` as watermark
- **Optimization**: Avoids redundant full reloads, only processes new/changed records

#### **3. Silver Layer (Cleansed/Conformed)**
- **Purpose**: Data cleansing, transformation, and business logic application
- **Schema**: `project_1_data_modeling.silver`
- **Function**: Applies transformations and adds metadata columns
- **Transformations Implemented**:
  - Customer name standardization (UPPER case conversion)
  - Addition of `processed_date` timestamp for audit trails
- **Loading Pattern**: UPSERT (MERGE) operations for maintaining data consistency

#### **4. Gold Layer (Curated/Consumption)**
- **Purpose**: Business-ready analytical models (Star Schema)
- **Schema**: `project_1_data_modeling.gold`
- **Function**: Dimensional modeling with fact and dimension tables
- **Design Pattern**: Star Schema with surrogate keys

---



## 📊 Data Model Design

### Star Schema Implementation

The gold layer implements a **Star Schema** design optimized for analytical queries and reporting:

```
                    DimCustomer
                         |
                         |
    DimProduct ----  FactSales  ---- DimPayments
                         |
                         |
                    DimOrders
                         |
                         |
                    DimCountry
```

### Dimension Tables

#### **1. DimCustomer**
```sql
- DimCustomerKey (Surrogate Key - INT, Auto-increment)
- customer_id (Natural Key - INT)
- customer_name (VARCHAR)
- customer_email (VARCHAR)
```

#### **2. DimProduct**
```sql
- DimProductKey (Surrogate Key - INT, Auto-increment)
- product_id (Natural Key - VARCHAR)
- product_name (VARCHAR)
- product_category (VARCHAR)
```

#### **3. DimOrders**
```sql
- DimOrderKey (Surrogate Key - INT, Auto-increment)
- order_id (Natural Key - INT)
- order_date (DATE)
```

#### **4. DimCountry**
```sql
- DimCountryKey (Surrogate Key - INT, Auto-increment)
- country (VARCHAR)
```

#### **5. DimPayments**
```sql
- DimPaymentKey (Surrogate Key - INT, Auto-increment)
- payment_type (VARCHAR)
```

### Fact Table

#### **FactSales**
```sql
- DimOrderKey (Foreign Key)
- DimCustomerKey (Foreign Key)
- DimCountryKey (Foreign Key)
- DimPaymentKey (Foreign Key)
- DimProductKey (Foreign Key)
- quantity (INT - Measure)
- unit_price (DECIMAL - Measure)
```

### Why Surrogate Keys?

The project implements **surrogate keys** (auto-incrementing integers) for all dimension tables instead of using natural business keys. This design decision provides several critical advantages:

1. **Stability**: Source system IDs can change or be reused; surrogate keys remain stable forever
2. **Performance**: Integer-based joins are significantly faster than text or composite key joins, especially at scale
3. **SCD Flexibility**: Enables seamless transition from SCD Type 1 to Type 2 without schema redesign
4. **Source Independence**: Decouples the data warehouse from source system changes
5. **Consistency**: Provides uniform key structure across all dimensions regardless of source formats

---



## 🔄 ETL Pipeline Implementation

### Phase 1: Initial Data Load

#### Step 1: Source Layer Setup
```sql
CREATE TABLE project_1_data_modeling.default.source_data (
    order_id INT PRIMARY KEY,
    order_date DATE,
    customer_id INT,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    payment_type VARCHAR(50),
    country VARCHAR(50),
    last_updated DATE
);
```

**Initial Data Load**: Inserted 3 sample records with order dates of `2024-07-01`

#### Step 2: Bronze Layer - Incremental Load Logic
```python
# Check if bronze table exists and get last processed date
if spark.catalog.tableExists('project_1_data_modeling.bronze.bronze_source_table'):
    last_load_date = spark.sql(
        "SELECT max(order_date) FROM project_1_data_modeling.bronze.bronze_source_table"
    ).collect()[0][0]
else:
    last_load_date = '1000-01-01'  # Initial load
```

**Key Innovation**: Implements watermark-based incremental loading to process only new records, eliminating redundant data processing and optimizing resource utilization.

```python
# Create temp view with only new records
spark.sql(f"""
    SELECT * FROM project_1_data_modeling.default.source_data
    WHERE order_date > '{last_load_date}'
""").createOrReplaceTempView('bronze_source_view')

# Load into bronze table
CREATE OR REPLACE TABLE project_1_data_modeling.bronze.bronze_source_table 
AS SELECT * FROM bronze_source_view
```

#### Step 3: Silver Layer - Transformation and UPSERT
```python
# Apply transformations
spark.sql("""
    SELECT *,
        UPPER(customer_name) as customer_name_upper,
        DATE(current_timestamp()) as processed_date
    FROM project_1_data_modeling.bronze.bronze_source_table
""").createOrReplaceTempView("silver_source_view")
```

**MERGE Operation** (UPSERT Pattern):
```sql
MERGE INTO project_1_data_modeling.silver.silver_source_table
USING silver_source_view
ON project_1_data_modeling.silver.silver_source_table.order_id = silver_source_view.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Why MERGE?**
- Handles both INSERT (new records) and UPDATE (changed records) in a single atomic operation
- Prevents duplicate records
- Maintains data consistency and idempotency

### Phase 2: Incremental Load Testing

#### Step 4: Second Data Injection
```sql
INSERT INTO project_1_data_modeling.default.source_data VALUES
(1000004, '2024-07-02', 20004, 'David Lee', 'david@abc.com', 
 'SKU504', 'Samsung S23', 'Electronics', 1, 899.99, 'Credit Card', 'USA', '2024-07-02'),
(1000005, '2024-07-02', 20001, 'Alice Johnson', 'alice@gmail.com', 
 'SKU503', 'Nike Shoes', 'Footwear', 2, 129.99, 'Credit Card', 'USA', '2024-07-02');
```

**Result**: The incremental load logic successfully identified and processed only the 2 new records (order_date > '2024-07-01'), demonstrating efficient delta processing.

#### Step 5: Gold Layer - Dimensional Modeling

**Dimension Table Creation** (Example: DimCustomer):
```sql
CREATE TABLE project_1_data_modeling.gold.dimcustomer (
    DimCustomerKey INT GENERATED ALWAYS AS IDENTITY,
    customer_id INT,
    customer_name STRING,
    customer_email STRING
);

INSERT INTO project_1_data_modeling.gold.dimcustomer (customer_id, customer_name, customer_email)
SELECT DISTINCT customer_id, customer_name, customer_email
FROM project_1_data_modeling.silver.silver_source_table;
```

**Fact Table Creation with Joins**:
```sql
CREATE OR REPLACE TABLE project_1_data_modeling.gold.FactSales AS
SELECT 
    o.DimOrderKey,
    c.DimCustomerKey,
    co.DimCountryKey,
    p.DimPaymentKey,
    pr.DimProductKey,
    s.quantity,
    s.unit_price
FROM project_1_data_modeling.silver.silver_source_table s
LEFT JOIN project_1_data_modeling.gold.dimcustomer c
    ON s.customer_id = c.customer_id
LEFT JOIN project_1_data_modeling.gold.dimcountry co
    ON s.country = co.country
LEFT JOIN project_1_data_modeling.gold.dimpayments p
    ON s.payment_type = p.payment_type
LEFT JOIN project_1_data_modeling.gold.dimproducts pr
    ON s.product_id = pr.product_id
LEFT JOIN project_1_data_modeling.gold.dimorders o
    ON s.order_id = o.order_id;
```

**Benefits of This Design**:
- **Query Performance**: Star schema enables fast aggregations and filtering
- **Storage Efficiency**: Dimension data is stored once and referenced via foreign keys
- **Scalability**: Fact table can grow to billions of rows while dimension tables remain relatively small
- **Reporting Flexibility**: Easy to add new dimensions without restructuring fact table

---

## 🔄 Slowly Changing Dimension (SCD) Implementation

### SCD Type 1: Latest Value Only (No History)

**Use Case**: When only the current state matters and historical values are not needed for analysis (e.g., correcting data entry errors, updating customer contact information)

#### Implementation Steps:

**1. Source Table Setup**:
```sql
CREATE TABLE project_1_data_modeling.default.scdtype1_source (
    product_id INT,
    product_name STRING,
    product_category STRING,
    last_updated TIMESTAMP
);

INSERT INTO project_1_data_modeling.default.scdtype1_source VALUES
(1, 'product_1', 'category_1', current_timestamp()),
(2, 'product_2', 'category_2', current_timestamp()),
(3, 'product_3', 'category_3', current_timestamp());
```

**2. Target Table Creation**:
```sql
CREATE TABLE project_1_data_modeling.gold.scdtype1_table (
    product_id INT,
    product_name STRING,
    product_category STRING,
    last_updated TIMESTAMP
);
```

**3. MERGE Logic with Timestamp Check**:
```sql
MERGE INTO project_1_data_modeling.gold.scdtype1_table target
USING scdtype1_source
ON target.product_id = scdtype1_source.product_id
WHEN MATCHED AND scdtype1_source.last_updated > target.last_updated
    THEN UPDATE SET *
WHEN NOT MATCHED 
    THEN INSERT *
```

**Critical Logic**: The condition `scdtype1_source.last_updated > target.last_updated` ensures updates only occur when source data is actually newer, preventing unnecessary write operations.

**4. Testing the Update**:
```sql
-- Update product_id = 3 category
UPDATE project_1_data_modeling.default.scdtype1_source
SET product_category = 'electronics', last_updated = current_timestamp()
WHERE product_id = 3;
```

**Result**: After re-running the MERGE, product_id = 3 shows the updated category 'electronics', and the old value 'category_3' is overwritten completely.

**SCD Type 1 Characteristics**:
- ✅ Simple to implement and maintain
- ✅ Minimal storage requirements
- ✅ Always shows current state
- ❌ No historical tracking
- ❌ Cannot analyze trends over time
- **Best For**: Reference data, corrections, attributes that don't require history

---

### SCD Type 2: Full Historical Tracking

**Use Case**: When you need to track changes over time for trend analysis, compliance, auditing, or point-in-time reporting (e.g., price changes, customer address changes, product category reclassifications)

#### Implementation Steps:

**1. Source Table Setup**:
```sql
CREATE TABLE project_1_data_modeling.default.scdtype2_source (
    product_id INT,
    product_name STRING,
    product_category STRING,
    last_updated TIMESTAMP
);

INSERT INTO project_1_data_modeling.default.scdtype2_source VALUES
(1, 'product_1', 'category_1', current_timestamp()),
(2, 'product_2', 'category_2', current_timestamp()),
(3, 'product_3', 'category_3', current_timestamp());
```

**2. Target Table with SCD Type 2 Columns**:
```sql
CREATE TABLE project_1_data_modeling.gold.scdtype2_table (
    product_id INT,
    product_name STRING,
    product_category STRING,
    last_updated TIMESTAMP,
    -- SCD Type 2 tracking columns
    start_date TIMESTAMP,      -- When this version became active
    end_date TIMESTAMP,        -- When this version expired (3000-01-01 for current)
    is_active STRING           -- 'Y' for current, 'N' for historical
);
```

**3. Source View Preparation**:
```python
spark.sql("""
    SELECT *,
        current_timestamp AS start_date,
        CAST('3000-01-01' AS timestamp) AS end_date,
        'Y' AS is_active
    FROM project_1_data_modeling.default.scdtype2_source
""").createOrReplaceTempView("src")
```

**4. Two-Step MERGE Process**:

**Step A: Expire Old Records**
```sql
MERGE INTO project_1_data_modeling.gold.scdtype2_table AS t
USING src
ON t.product_id = src.product_id AND t.is_active = 'Y'
WHEN MATCHED AND (
    src.product_name <> t.product_name OR
    src.product_category <> t.product_category OR
    src.last_updated <> t.last_updated
)
THEN UPDATE SET
    t.end_date = current_timestamp(),
    t.is_active = 'N'
```

**Logic Explanation**:
- Matches on `product_id` AND `is_active = 'Y'` to find current active records
- Checks if any tracked attributes have changed
- If changed, expires the current record by setting `end_date` to now and `is_active` to 'N'

**Step B: Insert New Records**
```sql
MERGE INTO project_1_data_modeling.gold.scdtype2_table AS t
USING src
ON t.product_id = src.product_id AND t.is_active = 'Y'
WHEN NOT MATCHED
THEN INSERT (
    product_id, product_name, product_category, last_updated,
    start_date, end_date, is_active
) VALUES (
    src.product_id, src.product_name, src.product_category, src.last_updated,
    src.start_date, src.end_date, src.is_active
)
```

**Logic Explanation**:
- For records that don't have an active version (either brand new or just expired)
- Inserts a new row with current values
- Sets `start_date` to now, `end_date` to far future, and `is_active` to 'Y'

**5. Testing Historical Tracking**:
```sql
-- Update product_id = 3 category
UPDATE project_1_data_modeling.default.scdtype2_source
SET product_category = 'new_category'
WHERE product_id = 3;

-- Run both MERGE steps again
```

**Result**: 
- Old record: `product_id=3, category='category_3', is_active='N', end_date=<timestamp>`
- New record: `product_id=3, category='new_category', is_active='Y', end_date='3000-01-01'`

**SCD Type 2 Characteristics**:
- ✅ Complete history preservation
- ✅ Point-in-time reporting capabilities
- ✅ Trend analysis and auditing
- ✅ Compliance-ready
- ❌ Increased storage requirements
- ❌ More complex queries (need to filter for active records)
- **Best For**: Price history, customer attributes, product classifications, any data requiring historical analysis

---

## 🛠️ Technical Implementation Details

### Technologies Used
- **Platform**: Databricks (Free Edition)
- **Compute**: Databricks Workspace with Apache Spark
- **Languages**: 
  - SQL (DDL, DML, MERGE operations)
  - PySpark (for dynamic query generation and temp views)
- **Storage**: Delta Lake tables (ACID transactions, time travel)

### Key Databricks Features Leveraged

1. **Magic Commands**: 
   - `%sql` - Execute SQL directly in notebooks
   - `%python` - PySpark code execution
   - `%md` - Markdown documentation within notebooks

2. **Unity Catalog**: 
   - Three-level namespace: `catalog.schema.table`
   - Centralized governance and metadata management

3. **Delta Lake Advantages**:
   - ACID transactions for reliability
   - MERGE operations for efficient upserts
   - Schema enforcement and evolution
   - Time travel for data versioning
