import dlt

#Expectations Cusomer table rules
customers_rules = {
    "rules_1": "customer_id IS NOT NULL",
    "rules_2" : "customer_name IS NOT NULL",
}

#create customers staging table
@dlt.table(
    name = "customers_stg"
)
#create expectations for customers staging table
@dlt.expect_all_or_drop(customers_rules)

#create function to read customers source table
def customers_stg():
  df = spark.readStream.table("dltbahtee.source.customers")
  return df