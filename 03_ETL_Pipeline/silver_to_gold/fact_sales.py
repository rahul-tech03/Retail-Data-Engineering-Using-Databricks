from pyspark.sql.functions import upper, trim, sum as _sum, countDistinct, col
from pyspark import pipelines as dp

@dp.table(name="retail_q.retail_gold.fact_sales")
def fact_sales():
    transactions_df = spark.read.table("retail_q.retail_silver.transactions")
    opportunity_df = spark.read.table("retail_q.retail_silver.opportunity")
    
    joined_df = transactions_df.alias("t").join(
        opportunity_df.alias("o"),
        upper(trim(transactions_df.opportunity_name)) == upper(trim(opportunity_df.name)),
        how="left"
    )
    
    # Select important columns (customize as needed)
    selected_df = joined_df.select(
        "t.transaction_id",
        "t.opportunity_name",
        "t.product_id",
        "t.store_id",
        "t.quantity",
        "t.selling_price",
        "t.discount_amount",
        "t.transaction_timestamp",
        col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        "t.payment_mode",
        "t.sales_channel",
        "o.name",
        "o.stage_name",
        "o.owner_id",
        "o.amount",
        col("o.account_id").alias("customer_id")
    )
    
    return selected_df
