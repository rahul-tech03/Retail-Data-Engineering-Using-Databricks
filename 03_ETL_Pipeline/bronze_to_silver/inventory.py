from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.inventory",
    comment="Inventory data with profiled columns and data quality checks"
)
@dp.expect_or_drop("non-null inventory_id", "inventory_id IS NOT NULL")
@dp.expect("valid stock_quantity", "stock_quantity > 0")
@dp.expect("non-null product_id", "product_id IS NOT NULL")
@dp.expect("non-null store_id", "store_id IS NOT NULL")
def inventory_clean():
    # Read source streaming table
    source_df = spark.readStream.table("retail_q.postgres_bronze.inventory")
    
    # Select relevant columns with lowercase naming
    return (
        source_df.select(
            F.col("inventory_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity"),
            F.col("reorder_level"),
            F.when(
                F.col("stock_quantity") < F.col("reorder_level"),
                "LOW_STOCK"
            ).otherwise("HEALTHY").alias("inventory_status"),
            F.col("warehouse_location"),
            F.col("last_stock_update")
        )
    )
