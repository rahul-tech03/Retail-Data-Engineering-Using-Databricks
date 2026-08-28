from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.account",
    comment="Salesforce account data with core business columns and data quality checks"
)
@dp.expect_or_drop("non-null id", "id IS NOT NULL")
@dp.expect("non-null name", "customer_name IS NOT NULL")
def account_clean():
    # Read source streaming table
    source_df = spark.readStream.table("retail_q.salesforce_bronze.account")
    
    # Select core business columns with lowercase underscore naming
    return source_df.select(
        F.col("Id").alias("id"),
        F.col("IsDeleted").alias("is_deleted"),
        F.upper(F.trim(F.col("Name"))).alias("customer_name"),
        F.col("Type").alias("type"),
        F.col("ParentId").alias("parent_id"),
        F.col("BillingStreet").alias("billing_street"),
        F.col("BillingCity").alias("billing_city"),
        F.col("BillingState").alias("billing_state"),
        F.col("BillingPostalCode").alias("billing_postal_code"),
        F.col("BillingCountry").alias("billing_country"),
        F.col("ShippingStreet").alias("shipping_street"),
        F.col("ShippingCity").alias("shipping_city"),
        F.col("ShippingState").alias("shipping_state"),
        F.col("ShippingPostalCode").alias("shipping_postal_code"),
        F.col("ShippingCountry").alias("shipping_country"),
        F.col("Phone").alias("phone"),
        F.col("Website").alias("website"),
        F.coalesce(F.col("Industry"), F.lit("UNKNOWN")).alias("industry"),
        F.col("AnnualRevenue").alias("annual_revenue"),
        F.col("NumberOfEmployees").alias("number_of_employees"),
        F.col("Description").alias("description"),
        # Compute is_active: True when __END_AT is null (active record), False otherwise
        F.when(F.col("__END_AT").isNull(), True).otherwise(False).alias("is_active")
    )
