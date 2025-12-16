import dlt
from pyspark.sql.functions import *

#Create transformation view for sales_stg table
@dlt.view(
    name = "sales_enr_view",
    comment = "It is a transformation view for sales_enr"
)
#create modified column to transform sales_stg table
def sales_stg_trns():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("sales_amount", col("quantity") * col("amount"))
    return df 

#Create sales_enr table with auto cdc flow
dlt.create_streaming_table(
    name = "sales_enr"
)
#Create auto cdc flow from sales_enr_view to sales_enr table for SCD Type 1
dlt.create_auto_cdc_flow(
    target = "sales_enr",
    source = "sales_enr_view",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
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