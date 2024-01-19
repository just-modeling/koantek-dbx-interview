# Databricks notebook source
# MAGIC %md
# MAGIC 1. Catalog deployment

# COMMAND ----------

displayHTML("""<div style="width: 960px; height: 720px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:960px; height:720px" src="https://lucid.app/documents/embedded/0ae9579c-9bf5-4f4c-9c53-12460597e0a1" id="WxV6RxlbU332"></iframe></div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Entity Relation Diagram

# COMMAND ----------

displayHTML("""<div style="width: 960px; height: 720px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:960px; height:720px" src="https://lucid.app/documents/embedded/df3b8ade-ae7a-4d32-8d0f-2d017d39b949" id="V1V6r3CmjDLe"></iframe></div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Seed the entity relation

# COMMAND ----------

# MAGIC %run ./_seed

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Sample queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC order_date,
# MAGIC count(*) as num_orders
# MAGIC FROM db_shop.dbo.`order`
# MAGIC GROUP BY order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC order_date,
# MAGIC sum(order_amount) as order_amount_total
# MAGIC FROM db_shop.dbo.`order`
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC litm.productid,
# MAGIC weekofyear(odr.order_date) as weekid,
# MAGIC count(*) as num_orders
# MAGIC FROM db_shop.dbo.`lineitem` litm
# MAGIC LEFT JOIN db_shop.dbo.`order` odr 
# MAGIC   ON litm.orderid = odr.orderid
# MAGIC WHERE litm.returnflag = 'N'
# MAGIC AND litm.status = 'O'
# MAGIC GROUP BY litm.productid, weekofyear(odr.order_date)
# MAGIC ORDER BY litm.productid, weekofyear(odr.order_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT productid, num_orders FROM
# MAGIC (
# MAGIC   SELECT 
# MAGIC   litm.productid,
# MAGIC   sum(case when (
# MAGIC     odr.order_date <= date_sub(
# MAGIC       current_date(), 
# MAGIC       dayOfMonth(current_date())
# MAGIC       ) 
# MAGIC     AND 
# MAGIC     odr.order_date > date_sub(
# MAGIC       date_sub(current_date(), dayOfMonth(current_date())), 
# MAGIC       dayOfMonth(date_sub(current_date(), dayOfMonth(current_date())))
# MAGIC       )
# MAGIC     ) then 1 else 0 end) AS num_orders
# MAGIC   FROM db_shop.dbo.`lineitem` litm
# MAGIC   LEFT JOIN db_shop.dbo.`order` odr 
# MAGIC     ON litm.orderid = odr.orderid
# MAGIC   WHERE litm.returnflag = 'N'
# MAGIC   AND litm.status = 'O'
# MAGIC   GROUP BY litm.productid
# MAGIC ) a
# MAGIC ORDER BY num_orders DESC
# MAGIC LIMIT 20
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Sample Solution Diagram

# COMMAND ----------

displayHTML("""<div style="width: 960px; height: 720px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:960px; height:720px" src="https://lucid.app/documents/embedded/3b0406b8-3f0d-4b29-9e91-5cbd1e01c638" id="x.U6gpUK0~j."></iframe></div>""")

# COMMAND ----------


