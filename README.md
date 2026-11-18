# E-Commerce Data Engineering Pipeline using Databricks

## Project Overview

This project implements an end-to-end E-Commerce Data Engineering Pipeline using **Databricks, PySpark, Delta Lake, and Spark Declarative Pipelines (SDP)**.

The main objective is to build a structured and reliable data pipeline that ingests raw nested JSON data, transforms it into an analytical structure, applies data quality checks, and organizes the processed data into dimension and fact tables.

The project follows the **Medallion Architecture**, consisting of Bronze, Silver, and Gold layers.

It also incorporates hash-based surrogate keys, incremental data ingestion, and Slowly Changing Dimension Type 2 (SCD Type 2) for customer history management.

## Project Objectives

- Ingest raw E-Commerce JSON data into Databricks.
- Implement the Medallion Architecture using Bronze, Silver, and Gold layers.
- Flatten nested JSON structures and transform raw data into structured records.
- Apply data quality checks and validation rules.
- Design dimension and fact tables for structured data modeling.
- Implement SHA-256 hash-based surrogate keys.
- Implement SCD Type 2 for tracking changes in customer information.
- Test pipeline execution and incremental data ingestion.
- Maintain a clean and organized project structure.

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Programming language |
| PySpark | Distributed data processing |
| Databricks | Cloud data engineering platform |
| Spark Declarative Pipelines (SDP) | Pipeline development and management |
| Delta Lake | Reliable data storage and table management |
| Auto Loader | Incremental file ingestion |
| SQL | Data validation and querying |
| Git and GitHub | Version control and project hosting |

## Architecture

The project follows the Medallion Architecture.

**Raw JSON Data → Bronze → Silver → Gold**

### 1. Bronze Layer

The Bronze layer stores the ingested source data.

Implementation:

- Ingests nested E-Commerce JSON files using Databricks Auto Loader.
- Uses an explicitly defined schema for structured ingestion.
- Preserves the original nested data structure.
- Maintains ingestion metadata such as ingestion timestamp and source file path.

**Objective:** Create a reliable raw-data layer that serves as the starting point for further processing.

### 2. Silver Layer

The Silver layer transforms raw Bronze data into a structured format.

Implementation:

- Reads data from the Bronze layer.
- Flattens nested JSON objects such as order, customer, product, payment, return, and financial information.
- Converts date fields into appropriate date formats.
- Applies required data transformations.
- Creates useful derived columns for business analysis.
- Applies data quality expectations.
- Handles invalid records according to the configured validation rules.

**Data Quality Checks**

Examples of validation rules include:

- Order ID must not be NULL.
- Customer ID must not be NULL.
- Product ID must not be NULL.
- Quantity must be greater than zero.
- Product price must be non-negative.
- Total amount must be non-negative.
- Delivery date should not precede the order date.
- Return request date should not precede the order date.

The project uses SDP expectations with warning and drop behavior, depending on the validation rule.

**Objective:** Produce clean, structured, and validated data for the Gold layer.

### 3. Gold Layer

The Gold layer organizes processed data into dimension and fact tables.

**Dimension Tables**

The project implements dimension tables for customer, product, and payment information, along with the date dimension where configured in the final implementation.

**Fact Tables**

Fact tables store transactional information and associated measures. The project uses separate fact tables for its implemented business processes.

The fact tables are connected to the relevant dimensions using surrogate keys.

**Hash-Based Surrogate Keys**

The project uses SHA-256 hashing to generate deterministic surrogate keys.

These keys provide consistent identifiers for dimension records and support relationships between dimension and fact tables.

**Slowly Changing Dimension Type 2 (SCD Type 2)**

SCD Type 2 is implemented for the customer dimension to preserve historical changes in customer attributes.

When tracked customer information changes:

- The previous record is retained as historical data.
- The previous version is marked as no longer current.
- A new version is created with updated customer information.
- The new version is marked as the current record.

This approach maintains customer history instead of simply overwriting previous values.

## Source Dataset

The project uses an E-Commerce dataset in nested JSON format.

The source contains the following main objects:

- `order`
- `customer`
- `product`
- `payment`
- `return`
- `financials`

The dataset includes information such as order dates, customer details, product categories, payment methods, return information, discounts, quantities, shipping costs, and profit margins.

The original dataset is not included in this repository because of its size. Small sample JSON files are provided in the `test_data/` directory for testing and demonstration.

## Project Structure

```text
ecommerce-data-engineering-databricks/
│
├── README.md
├── src/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
│
├── test_data/
│   ├── test_01_new_records.json
│   └── test_02_customer_update.json
│
├── screenshots/
│
├── docs/
│
├── .gitignore
└── LICENSE
```

## Testing Strategy

The pipeline is tested layer by layer to validate ingestion, transformation, data quality, table relationships, and historical data management.

### Phase 1: Layer-wise Testing

1. **Bronze Ingestion Testing**
   - Verify successful JSON ingestion.
   - Validate nested columns and schema.
   - Check ingestion metadata.
   - Verify that the expected records are available.

2. **Silver Transformation and Data Quality Testing**
   - Validate flattened columns.
   - Check data types and date conversions.
   - Verify data quality expectations.
   - Check invalid and missing values.

3. **Dimension Table Testing**
   - Validate dimension records.
   - Check natural keys and surrogate keys.
   - Verify uniqueness and expected relationships.

4. **Fact Table Testing**
   - Validate transactional records and measures.
   - Verify dimension key relationships.
   - Check record counts and important business values.

5. **SCD Type 2 Testing**
   - Insert a new customer.
   - Change tracked customer attributes.
   - Verify that the previous version is preserved.
   - Verify that a new current version is created.

### Phase 2: Incremental Ingestion Testing

Additional JSON files are uploaded separately to the source location to verify that Auto Loader detects and processes new files.

The pipeline is executed again to validate incremental processing.

### Phase 3: GitHub Preparation

- Organize source files.
- Remove temporary and unnecessary files.
- Exclude credentials and sensitive information.
- Add documentation and screenshots.
- Publish the project using GitHub.

**Testing Status:** Test results will be documented after the corresponding tests have been executed and verified.

## Screenshots

Screenshots demonstrating the project implementation and execution will be added here.

Suggested evidence:

1. Pipeline architecture.
2. Successful pipeline execution.
3. Bronze layer output.
4. Silver layer output.
5. Gold dimension tables.
6. Gold fact tables.
7. SCD Type 2 customer history.

## How to Use This Project

1. Clone or download this repository.
2. Import the source files into your Databricks workspace.
3. Configure the required source data location and pipeline settings.
4. Upload the source JSON dataset to the configured Databricks storage location.
5. Configure and execute the Spark Declarative Pipeline.
6. Validate the Bronze, Silver, and Gold tables.
7. Use the sample files in `test_data/` to perform additional ingestion and SCD Type 2 tests.

**Note:** The source storage path, catalog, schema, pipeline configuration, and required Databricks permissions must be configured for the target environment. The repository does not contain cloud credentials or workspace-specific secrets.

## Key Learning Outcomes

Through this project, I explored and implemented:

- Nested JSON data ingestion.
- Distributed data processing using PySpark.
- Databricks Auto Loader.
- Spark Declarative Pipelines.
- Medallion Architecture.
- Data transformation and validation.
- Delta Lake tables.
- Dimension and fact modeling.
- SHA-256 hash-based surrogate keys.
- Slowly Changing Dimension Type 2.
- Incremental data ingestion.
- Pipeline testing and debugging.
- Git and GitHub project management.

## Author

**[Your Name]**

Data Engineering Project

---

*This project was developed as a practical learning project to explore modern data engineering concepts and implement an end-to-end data pipeline using Databricks.*
