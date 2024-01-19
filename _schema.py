# Databricks notebook source
from pyspark.sql.types import IntegerType,DecimalType,LongType,BooleanType,DateType,TimestampType,StringType,StructType,StructField,ShortType

customer_schema = StructType(
    [
        StructField("customerid", LongType(), False),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("created_timestamp", TimestampType(), False),
        StructField("updated_timestamp", TimestampType(), True)
    ]
)

product_schema = StructType(
    [
        StructField("productid", LongType(), False),
        StructField("product_name", StringType(), True),
        StructField("product_description", StringType(), True),
        StructField("price", DecimalType(10,2), True),
        StructField("created_timestamp", TimestampType(), False),
        StructField("updated_timestamp", TimestampType(), True)
    ]
)

order_schema = StructType(
    [
        StructField("orderid", LongType(), False),
        StructField("customerid", LongType(), False),
        StructField("order_date", DateType(), True),
        StructField("order_amount", DecimalType(10,2), True),
        StructField("status", StringType(), False),
        StructField("created_timestamp", TimestampType(), False),
        StructField("updated_timestamp", TimestampType(), True)
    ]
)

lineitem_schema = StructType(
    [
        StructField("orderid", LongType(), False),
        StructField("lineitemid", LongType(), False),
        StructField("productid", LongType(), False),
        StructField("quantity", DecimalType(10,2), True),
        StructField("lineitem_amount", DecimalType(10,2), True),
        StructField("returnflag", StringType(), True),
        StructField("shipment_date", DateType(), True),
        StructField("estimated_delivery_date", DateType(), True),
        StructField("actual_delivery_date", DateType(), True),
        StructField("status", StringType(), False),
        StructField("created_timestamp", TimestampType(), False),
        StructField("updated_timestamp", TimestampType(), True)
    ]
)


# COMMAND ----------


