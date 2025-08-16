# Landing-staging-transform workflow

## Overview 
This project is a bauplan implementation of a high level architecture divided in landing /staging / transformation architecture.

### Desired architecture 

- Landing Zone (S3)
  - Archive and debugging store 
  - Parquet and csv files

- Staging Zone
  - Loading parquet to Iceberg
  - Raw data in Iceberg format.
  - data quality testing

- Transform layer
  - data pipelines
  - transformation and aggregation

### Bauplan implementation  

**- Landing zone (S3)** - Dump all the files in S3 - the ingestion and the connectors to get to this zone is outside the scope of this repo.

**- Staging Zone (S3).** 
- open an import branch
- create Iceberg tables and import data
- the import branch is then merged into main


- **The Transform layer** 
- open a transform branch
- run a transformation pipeline and expectation tests 
- merge into main 

The branches remain open for future debug - this is an implementation preference. Rolling back is always possible because the platform provides automatic versioning upon commits  

# Run the project 
## Setup
### Python environment
We recommend using [uv](https://docs.astral.sh/uv/guides/install-python/) to manage the dependencies:

```bash
uv sync
```

### Bauplan
* [Join](https://app.bauplanlabs.com/sign-up) the bauplan sandbox, create your username and API key.
* Complete do the 3-min [tutorial](https://docs.bauplanlabs.com/en/latest/tutorial/index.html) to get familiar with the platform.
* When you gain access, public datasets (including the one used in this project) will be available for you to start building pipelines.


## Run it

```bash
cd src 
uv run end_to_end_flow.py
```
The script executes the entire data pipeline (~150 lines of Python) from start to finish. Let's break it down step by step.

### 🚀 High-Level Overview
The process can be divided into **two main stages**:
1. **Data Ingestion & Validation** → From the raw zone to the staging zone
2. **Data Transformation** → from the staging zone to the insight/application layer

### 🔍 Step-by-Step Breakdown
### **1 Data Ingestion & Validation**
- **The ingestion logic** is defined in the function `from_raw_to_staging` in the file `end_to_end_flow.py`
    - **Imports raw files** into the system (S3).
    - **Convert files to Iceberg tables** in a temporary import branch.
    - **Run automated data quality tests** using `data_quality_tests.py`.
    - **Validation check:**
        - ✅ If tests **pass**, the data is merged into the **staging zone**.
        - ❌ If tests **fail**, the branch remains open for debugging.
      
<img src="img/landing_staging.jpg" alt="landing_staging.jpg" width="1000">

### **2 Data Transformation**
- Once data is staged, it runs through the **transformation pipeline** using Bauplan.
- The transformation logic is defined in the file `manual_pipeline/models.py`.
- The pipeline enriches the data and creates mart tables that can be visualized as dashboards.
- Showcase: the pipeline calls **OpenAI's API** for data enrichment - We ask `gpt-4` to create category tags from the products from the product descriptions and append them as a new column to the table `product_data`.
- The output is stored as **new tables** in the insights layer: `top_selling_products` and `top_selling_suppliers`.


<img src="img/staging_transform.jpg" alt="staging_transform.jpg" width="1000">


### 📊 What Do We Get at the End?
By the end of the process, we have **eight new tables** in the data lake:
- **Imported tables:**
    - `datacamp.product_data`
    - `datacamp.supplier_sku_lookup`
    - `datacamp.transaction_line_item`
- **Transformed tables:**
    - `datacamp.top_selling_products`
    - `datacamp.top_selling_suppliers`
You can verify the tables by running:
```bash
bauplan table --namespace datacamp
```
To explore the schema of the new tables run these commands in your terminal:
```bash
bauplan table get datacamp.product_data
bauplan table get datacamp.supplier_sku_lookup
bauplan table get datacamp.top_selling_products
bauplan table get datacamp.top_selling_suppliers
bauplan table get datacamp.transaction_line_item
```

### 🔎 Exploring and Querying Data
If you want to explore the final datasets, simply run:
```bash
bauplan query "SELECT * FROM datacamp.product_data WHERE category_name == 'Retail'"
bauplan query "SELECT * FROM datacamp.top_selling_suppliers"

```
This pipeline sets the foundation for **web-based dashboards**, **integration with Snowflake**, and **other analytics tools**.

### Running the transformation pipeline with bauplan interactively
To run the pipeline - i.e. the DAG going from the table imported to the final marts -- you just need to create a [data branch](https://docs.bauplanlabs.com/en/latest/concepts/branches.html).
```bash
cd src/manual_pipeline
bauplan branch create <YOUR_USERNAME>.datacamp_dag
bauplan branch checkout <YOUR_USERNAME>.datacamp_dag
```
You can now run the DAG:
```bash
bauplan run --namespace datacamp
```
You will notice that Bauplan stream back data in real-time, so every print statement you will be visualized in your terminal.