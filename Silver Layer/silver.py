
from pyspark.sql.functions import *
from pyspark import pipelines as dp


# =====================================================
# SILVER TABLE
# =====================================================

@dp.table(name="silver")

# DROP EXPECTATIONS

@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_product_id",
    "product_id IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_quantity",
    "quantity > 0"
)

@dp.expect_or_drop(
    "valid_product_price",
    "product_price >= 0"
)

@dp.expect_or_drop(
    "valid_total_amount",
    "total_amount >= 0"
)

# WARN EXPECTATIONS

@dp.expect(
    "valid_delivery_date",
    """
    order_delivered_date IS NULL
    OR order_delivered_date >= order_date
    """
)

@dp.expect(
    "valid_return_request_date",
    """
    request_date IS NULL
    OR request_date >= order_date
    """
)

def silver():

    # Read Bronze
    df = spark.readStream.table("bronze")


    # =================================================
    # 1. FLATTEN NESTED COLUMNS
    # =================================================

    df = df.select(

        col("customer.customer_id").alias("customer_id"),
        col("customer.age").alias("customer_age"),
        col("customer.gender").alias("customer_gender"),
        col("customer.region").alias("customer_region"),

        col("order.order_id").alias("order_id"),

        to_date(
            col("order.order_date"),
            "d/M/yyyy"
        ).alias("order_date"),

        to_date(
            col("order.delivered_date"),
            "d/M/yyyy"
        ).alias("order_delivered_date"),

        col("product.product_id").alias("product_id"),
        col("product.category").alias("product_category"),
        col("product.price").alias("product_price"),

        col("payment.payment_method").alias("payment_method"),

        col("return.returned").alias("returned"),

        to_date(
            col("return.request_date"),
            "d/M/yyyy"
        ).alias("request_date"),

        col("return.return_reason").alias("return_reason"),

        col("financials.discount").alias("discount"),
        col("financials.quantity").alias("quantity"),
        col("financials.total_amount").alias("total_amount"),
        col("financials.shipping_cost").alias("shipping_cost"),
        col("financials.profit_margin").alias("profit_margin"),

        col("ingestion_timestamp"),
        col("source_file_path")
    )


    # =================================================
    # 2. CALCULATE DELIVERY DAYS
    # =================================================

    df = df.withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_date"),
            col("order_date")
        )
    )


    # =================================================
    # 3. CLASSIFY DELIVERY STATUS
    # =================================================

    df = df.withColumn(
        "delivery_status",

        when(
            col("order_delivered_date").isNull(),
            "Unknown"
        )

        .when(
            col("delivery_days") <= 2,
            "Fast"
        )

        .when(
            col("delivery_days") <= 5,
            "Normal"
        )

        .otherwise("Delayed")
    )


    # =================================================
    # 4. CONVERT RETURNED INTO 1 OR 0
    # =================================================

    df = df.withColumn(
        "is_returned",

        when(
            lower(trim(col("returned"))) == "yes",
            1
        )

        .when(
            lower(trim(col("returned"))) == "no",
            0
        )

        .otherwise(None)
    )


    # =================================================
    # 5. CALCULATE RETURN DAYS
    # =================================================

    df = df.withColumn(
        "return_days",
        datediff(
            col("request_date"),
            col("order_date")
        )
    )


    # =================================================
    # 6. CALCULATE GROSS ITEM VALUE
    # =================================================

    df = df.withColumn(
        "gross_item_value",
        col("product_price") * col("quantity")
    )


    # =================================================
    # 7. SILVER PROCESSING TIMESTAMP
    # =================================================

    df = df.withColumn(
        "silver_processed_timestamp",
        current_timestamp()
    )


    # =================================================
    # 8. RETURN FINAL DATAFRAME
    # =================================================

    return df
