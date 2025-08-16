import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import bauplan
    import pandas as pd

    client = bauplan.Client()
    return client, pd


@app.cell
def _(client, pd):
    # define the branch and construct the query
    branch = "main"  # pick your branch
    table_name = "titanic"  # prick your table name
    query = f"SELECT Sex, Name FROM {table_name}"

    # run the query with the bauplan client against a iceberg table in the lakehouse
    # and turn it into a pandas dataframe
    df: pd.DataFrame = client.query(ref=branch, query=query).to_pandas()
    # visualize the first 30 rows of the pandas dataframe
    df.head(10)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
