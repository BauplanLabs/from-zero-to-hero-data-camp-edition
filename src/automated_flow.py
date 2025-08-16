import argparse
import bauplan
import datetime
import re


def construct_branch_name(branch_name: str):
    """Generate a unique branch name with timestamp."""
    timestamp = datetime.datetime.now().isoformat(timespec="seconds").replace(":", "_")
    return f"{branch_name}_{timestamp}"


def extract_table_name(filename):
    """Extract the table name from the filename."""
    pattern = r"demo-data-\d{4}-\d{2}-\d{2}-(.*?)\.csv"
    match = re.search(pattern, filename)
    return match.group(1) if match else None


def import_data_in_iceberg(
    client: bauplan.Client,
    table_name: str,
    ref_branch: str,
    source_s3: str,
    namespace: str,
):
    """
    Imports data into an Iceberg table in Bauplan, creating or replacing the table as needed.

    This function checks if the specified table already exists in the given branch.
    If it does, the table is deleted and recreated. The function then imports data
    from the specified S3 location into the newly created table.

    Parameters:
    :param client bauplan.Client
    :param table_name str - The name of the Iceberg table to create or replace.
    :param ref_branch str - The branch in which the table will be created or replaced.
    :param source_s3_location str - The S3 location containing the source data for import.
    :param namespace str - The namespace under which the table will be created.
    :return None

    """
    try:
        client.create_table(
            table=table_name,
            search_uri=source_s3,
            branch=ref_branch,
            namespace=namespace,
            replace=True,
        )
        print(f"✅ Table '{table_name}' created successfully.")
        client.import_data(
            table=table_name,
            search_uri=source_s3,
            branch=ref_branch,
            namespace=namespace,
        )
        print(f"✅ Data imported in '{table_name}'.")

    except bauplan.exceptions.BauplanError as e:
        print(f"Error: {e}")
        raise Exception("🔴 The import did not work correctly")


def from_raw_to_staging(
    bpln_client: bauplan.Client,
    s3_source_folder: str,
    list_of_tables_to_import: list,
    import_branch: str,
    namespace: str,
):
    """
    Imports raw data files from S3 into a staging area using a dedicated import branch and namespace.
    Steps:
      1. Creates a raw import branch (with a timestamp).
      2. Creates the specified namespace if it does not exist.
      3. Imports each file (from list_of_tables_to_import) as an Iceberg table from s3_source_folder.
      4. Runs data quality tests on the 'transaction_line_item' table:
         - Ensures 'line_total' contains no nulls.
         - Checks that 'transaction_line_item_id' values are unique.
      5. Merges the import branch into 'main' if all tests pass.

    Parameters:
    :param  bpln_client: bauplan.Client - The Bauplan client instance.
    :param  s3_source_folder: str - S3 folder containing raw data files.
    :param  list_of_tables_to_import: list - Filenames to import.
    :param  namespace: str - Namespace for the imported tables.

    Raises:
    AssertionError: if any data quality test fails.

    """

    # import the files as Iceberg tables into the import branch
    for filename in list_of_tables_to_import:
        table_name = extract_table_name(filename)
        s3_source = f"{s3_source_folder}{filename}"
        import_data_in_iceberg(
            client=bpln_client,
            table_name=table_name,
            ref_branch=import_branch,
            source_s3=s3_source,
            namespace=namespace,
        )

    # Run data quality tests before merging them into the main branch
    print("👀: Running data quality tests...")

    # Check that there are no null values in the column customer_product_id in the table product_data
    null_rows = bpln_client.query(
        query="SELECT customer_product_id FROM product_data WHERE customer_product_id IS NULL",
        namespace=namespace,
        ref=import_branch,
    )
    assert null_rows.num_rows == 0, (
        "🔴: There are null values in 'customer_product_id' column."
    )


def from_staging_to_applications(
    bpln_client: bauplan.Client,
    pipeline_folder: str,
    namespace: str,
    transform_branch: str,
):
    """
    Runs the transformation pipeline from staging to the insight layer using Bauplan.
    Executes the pipeline located in pipeline_folder under the specified namespace,
    prints the resulting job state, and logs any errors encountered during execution.

    Parameters:
    :param bpln_client: bauplan.Client - The Bauplan client instance.
    :param pipeline_folder: str- Directory containing the pipeline to run.
    :param namespace: str - Namespace in which the pipeline is executed.

    Logs:
    Prints the job ID and state if successful; otherwise, prints an error message.

    """
    # run the transformation pipeline for the insight layer
    run_state = bpln_client.run(
        project_dir=pipeline_folder,
        ref=transform_branch,
        namespace=namespace,
    )
    print(f"This is the result for {run_state.job_id}: {run_state.job_status}")
    if run_state.job_status.lower() == "failed":
        raise Exception(
            f"Pipeline {run_state.job_id} run failed: {run_state.job_status}"
        )


def main(namespace_suffix: str, profile: str):
    # Instantiate a bauplan client with specified profile
    bpln_client = bauplan.Client(profile=profile)

    # get the username from the client
    username = bpln_client.info().user.username

    # Define the source s3 location for the Raw data
    # We use the standard one for the sandbox, no need to customize this
    s3_source_folder = "s3://alpha-hello-bauplan/data_camp_demo_data/"

    # define namespace in the data catalog
    namespace = f"{username}_{namespace_suffix}"

    # Define the list of files that need to be imported as Iceberg tables
    # We use the standard one for the sandbox, no need to customize this
    list_of_files = [
        "demo-data-2025-02-12-product_data.csv",
        "demo-data-2025-02-12-supplier_sku_lookup.csv",
        "demo-data-2025-02-12-transaction_line_item.csv",
    ]

    # construct the name of the import branch
    import_branch = construct_branch_name(branch_name=f"{username}.data_upload")
    # Create the import branch
    assert bpln_client.create_branch(branch=import_branch, from_ref="main"), (
        "Something went wrong while creating the import branch"
    )
    print(f"✅ Branch '{import_branch}' created.")

    # import the raw data into the staging zone
    from_raw_to_staging(
        bpln_client=bpln_client,
        import_branch=import_branch,
        s3_source_folder=s3_source_folder,
        list_of_tables_to_import=list_of_files,
        namespace=namespace,
    )
    # merge the import branch into the main branch
    assert bpln_client.merge_branch(source_ref=import_branch, into_branch="main"), (
        "Something went wrong while merging the import branch into main."
    )
    print(f"✅ Branch '{import_branch}' merged into main.")

    # construct the name of the transform branch
    transform_branch = construct_branch_name(
        branch_name=f"{username}.data_transformation"
    )

    # Create a raw import branch with a timestamp in the name
    assert bpln_client.create_branch(branch=transform_branch, from_ref="main"), (
        "Something went wrong while creating the transformation branch"
    )
    print(f"✅ Branch '{transform_branch}' created.")

    # run the transformation pipeline from staging to marts and applications
    from_staging_to_applications(
        bpln_client=bpln_client,
        transform_branch=transform_branch,
        pipeline_folder="bpln_pipeline",
        namespace=namespace,
    )
    # Remember: here we do NOT merge back to main, as it is a sandbox
    print("🐬 So long and thanks for all the fish!")

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated Bauplan data flow")
    parser.add_argument(
        "--namespace_suffix",
        type=str,
        help="Suffix for the namespace (will be combined with username as {username}_{suffix})",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="default",
        help="Bauplan profile name (default: 'default')",
    )

    args = parser.parse_args()
    main(args.namespace_suffix, args.profile)
