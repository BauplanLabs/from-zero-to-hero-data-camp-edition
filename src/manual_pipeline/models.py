import bauplan


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11", pip={"duckdb": "1.2.0"})
def top_selling_products(
    transaction_line_item=bauplan.Model("transaction_line_item"),
    product_data=bauplan.Model("product_data"),
):
    """
    Joins transaction line items with enriched product data and aggregates sales metrics.

    Output Table:
    | customer_product_id | product_name   | sku         | category_name   | total_units_sold | total_revenue |
    |---------------------|----------------|-------------|-----------------|------------------|---------------|
    | <id>                | <product name> | <sku>       | <category>      | <number>         | <number>      |

    """
    import duckdb

    results = duckdb.sql("""
    SELECT 
        t.customer_product_id,
        p.product_name,
        p.sku,
        p.category_name,
        SUM(t.order_quantity) AS total_units_sold,
        SUM(t.line_total) AS total_revenue
    FROM transaction_line_item t
    JOIN product_data p 
      ON t.customer_product_id = p.customer_product_id
    GROUP BY 
        t.customer_product_id, 
        p.product_name, 
        p.sku, 
        p.category_name
    ORDER BY total_revenue DESC
    """).arrow()

    return results


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11", pip={"duckdb": "1.2.0"})
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
    import duckdb

    results = duckdb.sql("""
     SELECT
        s.supplier_name,
        SUM(ps.total_revenue) AS total_supplier_revenue
    FROM top_selling_products ps
    JOIN supplier_sku_lookup s ON ps.sku = s.sku
    GROUP BY s.supplier_name
    ORDER BY total_supplier_revenue DESC
    LIMIT 10;
    """).arrow()

    return results
