import bauplan


def are_there_nulls(
    client: bauplan.Client,
    table_name: str,
    column_to_check: str,
    ingestion_branch: str,
    namespace: str,
):
    """
    We check the data quality by running a data quality test in-process: we use
    Bauplan SDK to scan a table in S3 as an Arrow table, and check if the
    target column is not null through vectorized PyArrow operations.
    Pro -- it is very fast. Con -- it is in Pyarrow and not in SQL.

    :param client: Instance of bauplanClient to execute the query.
    :param table_name: Name of the table to check.
    :param column_to_check: The column for which uniqueness should be verified.
    :param ingestion_branch: The branch reference for the query.
    :param namespace: The namespace where the table is located.
    :return: True if there are null values in the terget colum, False otherwise.
    """
    arrow_table = client.scan(
        table=table_name,
        ref=ingestion_branch,
        namespace=namespace,
        columns=[column_to_check],
    )
    return arrow_table[column_to_check].null_count > 0


def expect_column_values_to_be_unique(
    client: bauplan.Client,
    table_name: str,
    column_to_check: str,
    ingestion_branch: str,
    namespace: str,
):
    """
    Checks whether the values in a specified column of a table are unique.
    We use Bauplan SDK to query a table to get the total row count and the count of distinct values
    in the target column. If the two counts are equal, it means that all values in that column are unique.
    Pro -- it is in SQL so it is easy to customize the test.
    Con -- using client.query can be slow for very large tables.

    :param client: Instance of bauplan.Client to execute the query.
    :param table_name: Name of the table to check.
    :param column_to_check: The column for which uniqueness should be verified.
    :param ingestion_branch: The branch reference for the query.
    :param namespace: The namespace where the table is located.
    :return: True if all values are unique, False otherwise.
    """
    sql_query = f"""
        SELECT 
            COUNT(*) AS total_count, 
            COUNT(DISTINCT {column_to_check}) AS unique_count 
        FROM {table_name}
        """
    table = client.query(
        query=sql_query, ref=ingestion_branch, namespace=namespace
    ).to_pylist()

    return table[0]["total_count"] == table[0]["unique_count"]
