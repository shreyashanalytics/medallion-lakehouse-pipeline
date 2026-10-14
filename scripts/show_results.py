"""Print a few friendly results from the Gold layer (star schema)."""

import duckdb

con = duckdb.connect("/opt/airflow/warehouse/lakehouse.duckdb", read_only=True)

print("\nHow many rows are in each Gold table?")
for table in ["dim_date", "dim_customer", "dim_product", "fact_sales"]:
    count = con.sql(f"select count(*) from gold.{table}").fetchone()[0]
    print(f"  {table:<14} {count:>8,}")

print("\nTop 5 product categories by revenue:")
con.sql(
    """
    select p.category, round(sum(f.recognized_revenue), 2) as revenue
    from gold.fact_sales f join gold.dim_product p using (product_key)
    group by 1 order by 2 desc limit 5
    """
).show()
