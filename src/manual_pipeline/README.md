# Manual Pipeline Workflow

This pipeline demonstrates a simple data ingestion and querying workflow using Bauplan with Iceberg tables.

## Workflow Overview

### Step 1: Upload Data Files
Upload your CSV or Parquet files to an S3 bucket. For simplicity, we'll use the Bauplan UI for this step:
- Navigate to the Bauplan UI
- Select your target S3 bucket
- Upload your CSV/Parquet files directly through the interface

### Step 2: Import to Iceberg Table
Once the files are in S3, import them into an Iceberg table. This creates a queryable, versioned table format that supports:
- Schema evolution
- Time travel queries
- Efficient data management

### Step 3: Query the Data
After importing to Iceberg, you can query your data using multiple interfaces.


### Workflow step by step Upload a csv file to s3
Go to https://app.bauplanlabs.com/upload and upload the file `demo-data-2025-02-12-product_data.csv` and run the command printed in the UI.
This will create a data branch, a new Iceberg table and import data from your file into that table.
At the end of this process, you will have an Iceberg table. 
Check whether the table exists and see the difference between our newly created branch and the main.

```bash
bauplan table get <the_new_table>
bauplan branch diff
```

We now have an Iceberg table in our object storage. As simple as that. 

#### Query the table - Option A: Bauplan CLI
```bash
bauplan query "SELECT * FROM your_table_name LIMIT 10"
```

#### Option B: Integrated UI
Use the Bauplan web interface to:
- Write and execute SQL queries
- Visualize results

#### Option C: Python SDK in Notebooks
Run the notebook in this folder:
```bash
cd src/notebooks
jupter lab
```

### Import other tables

We already loaded the data in our s3, so we can just import them skipping the s3 upload
- Create a new branch - you will have to change the branch with your username and branch name.

```bash
bauplan checkout -b ciro.data_camp
```

Import `product_data`:

```bash
bauplan checkout -b ciro.data_camp
bauplan table create \
--search-uri s3://alpha-hello-bauplan/data_camp_demo_data/demo-data-2025-02-12-product_data.csv \
--name product_data --namespace datacamp && \
bauplan table import \
--search-uri s3://alpha-hello-bauplan/data_camp_demo_data/demo-data-2025-02-12-product_data.csv \
--name product_data --namespace datacamp
```

Import `sku_lookup`

```bash
bauplan table create \
--search-uri s3://alpha-hello-bauplan/data_camp_demo_data/demo-data-2025-02-12-supplier_sku_lookup.csv \
--name supplier_sku_lookup --namespace datacamp && \
bauplan table import \
--search-uri s3://alpha-hello-bauplan/data_camp_demo_data/demo-data-2025-02-12-supplier_sku_lookup.csv \
--name supplier_sku_lookup --namespace datacamp
```

Import `transaction_line_item`

```bash
bauplan table create \
--search-uri s3://alpha-hello-bauplan/data_camp_demo_data/demo-data-2025-02-12-transaction_line_item.csv \
--name transaction_line_item --namespace datacamp && \
bauplan table import \
--search-uri s3://alpha-hello-bauplan/data_camp_demo_data/demo-data-2025-02-12-transaction_line_item.csv \
--name transaction_line_item --namespace datacamp
```

