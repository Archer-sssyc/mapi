import sys
import json
from pyspark.sql import functions as F

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType

action = "/Users/fugui/Work/NWCD/ohla/mapi/resource/action.csv"
items = "/Users/fugui/Work/NWCD/ohla/mapi/resource/item.csv"

item_schema = StructType([
    StructField("item_id", StringType()),
    StructField("item_detail", StringType()),
])

action_schema = StructType([
    StructField("user_id", StringType()),
    StructField("item_id", StringType()),
])


@udf(returnType=StringType())
def extract_action(message_str):
    t = message_str.split("_!_")
    return json.dumps({
        "user_id": t[0],
        "item_id": t[1]
    })


@udf(returnType=StringType())
def extract_item(message_str):
    t = message_str.split("_!_")
    return json.dumps({
        "item_id": t[0],
        "detail": t[2]
    })


#
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# glueContext = GlueContext(sc)
# glue_spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

spark = SparkSession.builder.getOrCreate()
action_df = spark.read.format("text").option("multiLine", "false").text(action)
# filter empty row
action_df = action_df.filter("value is not null")
action_df.printSchema()
action_df = action_df.withColumn("data", extract_action(F.col("value")))
action_df = action_df.withColumn("data", F.from_json(F.col("data"), action_schema))
action_df = action_df.withColumn("user_id", F.col("data.user_id"))
action_df = action_df.withColumn("item_id", F.col("data.item_id"))
action_df = action_df.select("user_id", "item_id")
action_df.printSchema()
action_df.show(10)
# dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")
#
# glueContext.write_dynamic_frame.from_options(frame=dyn_df,
#                                              connection_type="dynamodb",
#                                              connection_options={"tableName": "demo-ohla-recommand"})

# job.commit()
