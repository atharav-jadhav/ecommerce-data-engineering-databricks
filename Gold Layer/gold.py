from pyspark.sql.functions import *
from pyspark import pipelines as dp


# 1. CUSTOMER CHANGE SOURCE

@dp.temporary_view(name="customer_changes")
def customer_changes():

    df = spark.readStream.table("silver")

    df = df.filter(
        col("customer_id").isNotNull() &
        col("order_date").isNotNull() &
        col("order_id").isNotNull()
    )

    df = df.select(

        trim(col("customer_id")).alias("customer_id"),

        col("customer_age"),
        col("customer_gender"),
        col("customer_region"),

        col("order_date"),
        col("order_id"),

        sha2(
            to_json(
                struct(
                    col("customer_age"),
                    col("customer_gender"),
                    col("customer_region")
                )
            ),
            256
        ).alias("hash_diff")

    )

    return df


# 2. CUSTOMER HISTORY - SCD TYPE 2

dp.create_streaming_table("customer_history_scd")

dp.create_auto_cdc_flow(

    target="customer_history_scd",

    source="customer_changes",

    keys=["customer_id"],

    sequence_by=struct("order_date", "order_id"),

    stored_as_scd_type="2",

    track_history_column_list=[
        "customer_age",
        "customer_gender",
        "customer_region"
    ],

    except_column_list=[
        "order_date",
        "order_id"
    ]

)


# 3. CUSTOMER DIMENSION

@dp.materialized_view(name="dim_customer")
def dim_customer():

    df = spark.read.table("customer_history_scd")

    df = df.withColumn(
        "customer_key",
        sha2(
            concat(
                col("customer_id"),
                lit("||"),
                to_json(col("__START_AT"))
            ),
            256
        )
    )

    df = df.withColumn(
        "effective_start_date",
        col("__START_AT.order_date")
    )

    df = df.withColumn(
        "effective_end_date",
        col("__END_AT.order_date")
    )

    df = df.withColumn(
        "effective_start_order_id",
        col("__START_AT.order_id")
    )

    df = df.withColumn(
        "effective_end_order_id",
        col("__END_AT.order_id")
    )

    df = df.withColumn(
        "is_current",
        col("__END_AT").isNull()
    )

    return df.select(

        "customer_key",
        "customer_id",

        "customer_age",
        "customer_gender",
        "customer_region",

        "effective_start_date",
        "effective_end_date",

        "effective_start_order_id",
        "effective_end_order_id",

        "is_current",
        "hash_diff",

        "__START_AT",
        "__END_AT"

    )


# 4. PRODUCT DIMENSION

@dp.materialized_view(name="dim_product")
def dim_product():

    df = spark.read.table("silver")

    df = df.select(
        "product_id",
        "product_category"
    )

    df = df.dropDuplicates(["product_id"])

    df = df.withColumn(
        "product_key",
        sha2(col("product_id"), 256)
    )

    return df


# 5. PAYMENT DIMENSION

@dp.materialized_view(name="dim_payment")
def dim_payment():

    df = spark.read.table("silver")

    df = df.select(
        lower(trim(col("payment_method")))
        .alias("payment_method")
    )

    df = df.filter(
        col("payment_method").isNotNull()
    )

    df = df.dropDuplicates(["payment_method"])

    df = df.withColumn(
        "payment_key",
        sha2(col("payment_method"), 256)
    )

    return df


# 6. DATE DIMENSION

@dp.materialized_view(name="dim_date")
def dim_date():

    df = spark.read.table("silver")

    dates = (
        df.select(
            col("order_date").alias("date")
        )
        .unionByName(
            df.select(
                col("order_delivered_date").alias("date")
            )
        )
        .unionByName(
            df.select(
                col("request_date").alias("date")
            )
        )
    )

    dates = dates.filter(
        col("date").isNotNull()
    )

    date_range = dates.agg(
        min("date").alias("min_date"),
        max("date").alias("max_date")
    )

    df = date_range.select(
        explode(
            sequence(
                col("min_date"),
                col("max_date"),
                expr("interval 1 day")
            )
        ).alias("date")
    )

    df = df.withColumn(
        "date_key",
        date_format(col("date"), "yyyyMMdd").cast("int")
    )

    df = df.withColumn(
        "day",
        dayofmonth(col("date"))
    )

    df = df.withColumn(
        "month",
        month(col("date"))
    )

    df = df.withColumn(
        "month_name",
        date_format(col("date"), "MMMM")
    )

    df = df.withColumn(
        "quarter",
        quarter(col("date"))
    )

    df = df.withColumn(
        "year",
        year(col("date"))
    )

    df = df.withColumn(
        "day_name",
        date_format(col("date"), "EEEE")
    )

    df = df.withColumn(
        "week_of_year",
        weekofyear(col("date"))
    )

    return df


# 7. FACT ORDERS

@dp.materialized_view(name="fact_orders")
def fact_orders():

    df = spark.read.table("silver").alias("s")

    customer_dim = spark.read.table("dim_customer").alias("c")
    product_dim = spark.read.table("dim_product").alias("p")
    payment_dim = spark.read.table("dim_payment").alias("pay")

    date_dim = spark.read.table("dim_date")

    order_date_dim = date_dim.alias("od")
    delivery_date_dim = date_dim.alias("dd")

    order_event = struct(
        col("s.order_date"),
        col("s.order_id")
    )

    df = df.join(
        customer_dim,

        (
            (trim(col("s.customer_id")) == col("c.customer_id")) &
            (order_event >= col("c.__START_AT")) &
            (
                (order_event < col("c.__END_AT")) |
                col("c.__END_AT").isNull()
            )
        ),

        "left"
    )

    df = df.join(
        product_dim,
        col("s.product_id") == col("p.product_id"),
        "left"
    )

    df = df.join(
        payment_dim,

        lower(trim(col("s.payment_method"))) ==
        col("pay.payment_method"),

        "left"
    )

    df = df.join(
        order_date_dim,
        col("s.order_date") == col("od.date"),
        "left"
    )

    df = df.join(
        delivery_date_dim,
        col("s.order_delivered_date") == col("dd.date"),
        "left"
    )

    df = df.select(

        col("s.order_id"),

        col("c.customer_key"),
        col("p.product_key"),
        col("pay.payment_key"),

        col("od.date_key").alias("order_date_key"),
        col("dd.date_key").alias("delivery_date_key"),

        col("s.quantity"),
        col("s.product_price"),
        col("s.discount"),
        col("s.total_amount"),
        col("s.shipping_cost"),
        col("s.profit_margin"),
        col("s.gross_item_value"),

        col("s.delivery_days"),
        col("s.delivery_status"),

        col("s.is_returned"),

        col("s.ingestion_timestamp"),
        col("s.source_file_path")

    )

    return df


# 8. FACT RETURNS

@dp.materialized_view(name="fact_returns")
def fact_returns():

    df = (
        spark.read.table("silver")
        .filter(col("is_returned") == 1)
        .alias("s")
    )

    customer_dim = spark.read.table("dim_customer").alias("c")
    product_dim = spark.read.table("dim_product").alias("p")
    payment_dim = spark.read.table("dim_payment").alias("pay")

    date_dim = spark.read.table("dim_date")

    order_date_dim = date_dim.alias("od")
    return_date_dim = date_dim.alias("rd")

    order_event = struct(
        col("s.order_date"),
        col("s.order_id")
    )

    df = df.join(
        customer_dim,

        (
            (trim(col("s.customer_id")) == col("c.customer_id")) &
            (order_event >= col("c.__START_AT")) &
            (
                (order_event < col("c.__END_AT")) |
                col("c.__END_AT").isNull()
            )
        ),

        "left"
    )

    df = df.join(
        product_dim,
        col("s.product_id") == col("p.product_id"),
        "left"
    )

    df = df.join(
        payment_dim,

        lower(trim(col("s.payment_method"))) ==
        col("pay.payment_method"),

        "left"
    )

    df = df.join(
        order_date_dim,
        col("s.order_date") == col("od.date"),
        "left"
    )

    df = df.join(
        return_date_dim,
        col("s.request_date") == col("rd.date"),
        "left"
    )

    df = df.select(

        col("s.order_id"),

        col("c.customer_key"),
        col("p.product_key"),
        col("pay.payment_key"),

        col("od.date_key").alias("order_date_key"),
        col("rd.date_key").alias("return_date_key"),

        col("s.return_reason"),
        col("s.return_days"),

        col("s.quantity"),
        col("s.product_price"),
        col("s.total_amount"),
        col("s.shipping_cost"),
        col("s.profit_margin"),

        col("s.ingestion_timestamp"),
        col("s.source_file_path")

    )

    return df
