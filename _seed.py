# Databricks notebook source
# MAGIC %run ./_schema
# MAGIC

# COMMAND ----------

# DBTITLE 1,Customer
from pyspark.sql import functions as F
from pyspark.sql import types as T

customer_seed = spark.read.table("samples.tpch.customer")
customer = customer_seed.select(["c_custkey","c_name","c_address","c_phone"])\
    .withColumnsRenamed({
        "c_custkey": "customerid",
        "c_name": "lastname",
        "c_address": "address",
        "c_phone": "phone"
    })\
    .withColumn("firstname", F.lit(None).cast(T.StringType()))\
    .withColumn("email", F.lit(None).cast(T.StringType()))\
    .withColumn("created_timestamp", F.current_timestamp())\
    .withColumn("updated_timestamp", F.lit(None).cast(T.TimestampType()))

spark.createDataFrame(data=[], schema=customer_schema)\
    .write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable("db_shop.dbo.customer")

customer.select([f.name for f in customer_schema.fields]).write.insertInto("db_shop.dbo.customer")

# COMMAND ----------

# DBTITLE 1,Product
from pyspark.sql import functions as F
from pyspark.sql import types as T

product_seed = spark.read.table("samples.tpch.part")
product = product_seed.select(["p_partkey","p_name","p_comment","p_retailprice"])\
    .withColumnsRenamed({
        "p_partkey": "productid",
        "p_name": "product_name",
        "p_comment": "product_description",
        "p_retailprice": "price"
    })\
    .withColumn("created_timestamp", F.current_timestamp())\
    .withColumn("updated_timestamp", F.lit(None).cast(T.TimestampType()))

spark.createDataFrame(data=[], schema=product_schema)\
    .write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable("db_shop.dbo.product")

product.select([f.name for f in product_schema.fields]).write.insertInto("db_shop.dbo.product")

# COMMAND ----------

# DBTITLE 1,Order
from pyspark.sql import functions as F
from pyspark.sql import types as T

order_seed = spark.read.table("samples.tpch.orders")
order = order_seed.select(["o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate"])\
    .withColumnsRenamed({
        "o_orderkey": "orderid",
        "o_custkey": "customerid",
        "o_orderstatus": "status",
        "o_totalprice": "order_amount",
        "o_orderdate": "order_date"
    })\
    .withColumn("created_timestamp", F.current_timestamp())\
    .withColumn("updated_timestamp", F.lit(None).cast(T.TimestampType()))

spark.createDataFrame(data=[], schema=order_schema)\
    .write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable("db_shop.dbo.order")

order.select([f.name for f in order_schema.fields]).write.insertInto("db_shop.dbo.order")

# COMMAND ----------

# DBTITLE 1,Lineitem
from pyspark.sql import functions as F
from pyspark.sql import types as T

lineitem_seed = spark.read.table("samples.tpch.lineitem")
lineitem = lineitem_seed.select(["l_orderkey","l_partkey","l_linenumber","l_quantity","l_extendedprice","l_shipdate","l_commitdate","l_receiptdate","l_linestatus","l_returnflag"])\
    .withColumnsRenamed({
        "l_orderkey": "orderid",
        "l_partkey": "productid",
        "l_linenumber": "lineitemid",
        "l_quantity": "quantity",
        "l_extendedprice": "lineitem_amount",
        "l_shipdate": "shipment_date",
        "l_commitdate": "estimated_delivery_date",
        "l_receiptdate": "actual_delivery_date",
        "l_linestatus": "status",
        "l_returnflag": "returnflag"
    })\
    .withColumn("created_timestamp", F.current_timestamp())\
    .withColumn("updated_timestamp", F.lit(None).cast(T.TimestampType()))

spark.createDataFrame(data=[], schema=lineitem_schema)\
    .write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable("db_shop.dbo.lineitem")

lineitem.select([f.name for f in lineitem_schema.fields]).write.insertInto("db_shop.dbo.lineitem")

# COMMAND ----------


