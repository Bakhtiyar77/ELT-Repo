import dlt
from pyspark.sql.functions import *

#Create business_sales table
@dlt.table(
  name = "business_sales"
)

#Create business sales transformation function
def business_sales():
  df_fact = spark.read.table("fact_sales")
  df_dimCustomer = spark.read.table("dim_customers")
  df_dimProduct = spark.read.table("dim_products")

  df_join = df_fact.join(df_dimCustomer, df_fact.customer_id == df_dimCustomer.customer_id, "inner").join(df_dimProduct, df_fact.product_id == df_dimProduct.product_id, "inner")
  df_prun = df_join.select("region", "category", "sales_amount")
  df_agg = df_prun.groupBy("region", "category").agg(sum("sales_amount").alias("total_sales"))
  return df_agg