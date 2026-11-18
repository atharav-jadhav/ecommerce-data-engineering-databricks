from pyspark.sql.functions import *
from pyspark import pipelines as dp
from pyspark.sql.types import *

my_schema = StructType([

    StructField(
        "order",
        StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("delivered_date", StringType(), True)
        ]),
        True
    ),

    StructField(
        "customer",
        StructType([
            StructField("customer_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("region", StringType(), True)
        ]),
        True
    ),

    StructField(
        "product",
        StructType([
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True)
        ]),
        True
    ),

    StructField(
        "payment",
        StructType([
            StructField("payment_method", StringType(), True)
        ]),
        True
    ),

    StructField(
        "return",
        StructType([
            StructField("returned", StringType(), True),
            StructField("request_date", StringType(), True),
            StructField("return_reason", StringType(), True)
        ]),
        True
    ),

    StructField(
        "financials",
        StructType([
            StructField("discount", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("shipping_cost", DoubleType(), True),
            StructField("profit_margin", DoubleType(), True)
        ]),
        True
    ),

    StructField("_rescued_data", StringType(), True),
    StructField("_corrupt_record", StringType(), True)

])


@dp.table(name="bronze")
def bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("mode", "PERMISSIVE")
        .schema(my_schema)
        .load("/Volumes/e-commerce_stream_flow/source/json_data/")
    )

    df = df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    df = df.withColumn(
        "source_file_path",
        col("_metadata.file_path")
    )

    return df
