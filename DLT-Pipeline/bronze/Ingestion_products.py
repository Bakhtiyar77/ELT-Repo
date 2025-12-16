import dlt


#Create product table expectations rules 
product_rules = {
  "rules_1": "product_id IS NOT NULL",
  "rules_2": "price >= 0"
}

#create products staging table
@dlt.table(
    name = "products_stg"
)

#create expectations for products staging table
@dlt.expect_all(product_rules)

#create function to read products source table
def products_stg():
  df = spark.readStream.table("dltbahtee.source.products")
  return df