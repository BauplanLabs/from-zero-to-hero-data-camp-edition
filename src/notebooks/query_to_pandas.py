import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import bauplan
    import pandas as pd

    client = bauplan.Client()
    username = client.info().user.username
    return client, pd, username


@app.cell
def _(client, pd, username):
    # define the branch and construct the query
    branch = f"{username}.transform_datacamp"  # make sure the branch is correct!
    namespace = f"{username}_data_camp"  # make sure the namespace is correct!
    query = "SELECT * FROM top_selling_suppliers"

    # run the query with the bauplan client against a iceberg table in the lakehouse
    # and turn it into a pandas dataframe
    df: pd.DataFrame = client.query(
        ref=branch, query=query, namespace=namespace
    ).to_pandas()
    # visualize the first 30 rows of the pandas dataframe
    df.head(10)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
