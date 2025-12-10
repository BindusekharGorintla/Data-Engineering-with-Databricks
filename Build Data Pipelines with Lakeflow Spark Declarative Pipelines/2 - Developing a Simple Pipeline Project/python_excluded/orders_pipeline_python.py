##
## PYTHON VERSION OF THE ORDERS_PIPELINE SQL FILE
## Documentation: https://docs.databricks.com/aws/en/ldp/developer/python-ref
##

## The syntax was formerly `import dlt`
from pyspark import pipelines as dp
import pyspark.sql.functions as F

source = spark.conf.get("source")


## A. Create the bronze streaming table in your labuser.1_bronze_db schema from a JSON files in your volume
  # NOTE: read_files references the 'source' configuration key from your pipeline settings. 
  # NOTE: 'source' = '/Volumes/dbacademy/ops/your-labuser-name'

@dp.table(name="1_bronze_db.orders_bronze_demo2")
def orders_bronze_demo2():
    return (
            spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                "*",
                F.current_timestamp().alias("processing_time"), 
                "_metadata.file_name"
            )
    )


# ## B. Create the silver streaming table in your labuser.2_silver_db schema (database)
@dp.table(name="2_silver_db.orders_silver_demo2")
def orders_silver_demo2():
    return (
        dp.read_stream("1_bronze_db.orders_bronze_demo2")
        .select(
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp"),
                "customer_id",
                "notifications"
            )
    )


# ## C. Create the materialized view aggregation from the orders_silver table with the summarization
@dp.materialized_view(name="3_gold_db.gold_orders_by_date_demo2")
def orders_by_date_gold_demo2():
    return (
        dp.read("2_silver_db.orders_silver_demo2")
        .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
        .agg(F.count("*").alias("total_daily_orders"))
    )