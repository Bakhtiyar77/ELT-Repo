import dlt
from pyspark.sql.functions import *

#create transformation view for products_stg table
@dlt.view(
    name = "products_enr_view",
    comment = "It is a transformation view for product_stg"
)
#create data type to transform products_stg table
def product_stg_trns():
    df = spark.readStream.table("products_stg")
    df = df.withColumn("price", col("price").cast("int"))
    return df 

dlt.create_streaming_table(
    name = "products_enr"
)

#Create auto cdc flow from products_enr_view to products_enr table for SCD Type 1
dlt.create_auto_cdc_flow(
    target = "products_enr",
    source = "products_enr_view",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
)