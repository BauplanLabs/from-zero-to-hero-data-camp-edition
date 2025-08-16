import bauplan


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11", pip={"polars": "1.15.0"})
def top_selling_suppliers(
    top_selling_products=bauplan.Model("top_selling_products"),
    supplier_sku_lookup=bauplan.Model("supplier_sku_lookup"),
):
    """
    Aggregates revenue by supplier to identify the top-selling suppliers.
    Joins the 'product_sales' table with the 'supplier_sku_lookup' table on the 'sku'
    field and sums the total revenue per supplier. and order results in descending order by revenue.

    Final Output Table:
    | supplier_name   | total_supplier_revenue |
    |-----------------|------------------------|
    | <supplier name>| <number>               |

    """
    import polars as pl
    
    df_products = pl.from_arrow(top_selling_products)
    df_suppliers = pl.from_arrow(supplier_sku_lookup)
    
    results_df = (
        df_products
        .join(df_suppliers, on="sku", how="inner")
        .group_by("supplier_name")
        .agg(pl.col("total_revenue").sum().alias("total_supplier_revenue"))
        .sort("total_supplier_revenue", descending=True)
    )

    return results_df.to_arrow()
