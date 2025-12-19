import dlt
from pyspark.sql.functions import *

#create transformation view for customers_stg table
@dlt.view(
    name = "customers_enr_view",
    comment = "It is a transformation view for customer_stg"
)

def product_stg_trns():
    df = spark.readStream.table("customers_stg")
    df = df.withColumn("customer_name", upper(col("customer_name")))
    return df 

#Create customers_enr table with auto cdc flow
dlt.create_streaming_table(
    name = "customers_enr"
)

#Create auto cdc flow from customers_enr_view to customers_enr table for SCD Type 1
dlt.create_auto_cdc_flow(
    target = "customers_enr",
    source = "customers_enr_view",
    keys = ["customer_id"],
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