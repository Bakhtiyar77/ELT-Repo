import dlt

#Expectations Sales table rules
sales_rules = {
     "rules_1" : "sales_id IS NOT NULL"
 }

#Create sales streaming table with expectations
dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop= sales_rules
)

#append east and west sales data to sales staging table
@dlt.append_flow(target = "sales_stg")
def east_sales():
    df = spark.readStream.table("dltbahtee.source.sales_east")
    return df


@dlt.append_flow(target = "sales_stg")
def west_sales():
    df = spark.readStream.table("dltbahtee.source.sales_west")
    return df