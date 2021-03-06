import sys
import json
from pyspark.sql import functions as F

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

action = "s3://example-data/ohla/action.csv"
item = "s3://example-data/ohla/item.csv"

action_schema = StructType([
    StructField("user_id", StringType()),
    StructField("item_id", StringType()),
])

item_schema = StructType([
    StructField("item_id", StringType()),
    StructField("item_desc", StringType()),
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
        "item_desc": t[2]
    })


#
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
glue_spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = SparkSession.builder.getOrCreate()
action_df = spark.read.format("text").option("multiLine", "false").text(action)
# filter empty row
action_df = action_df.filter("value is not null")
action_df.printSchema()
action_df = action_df.withColumn("data", extract_action(F.col("value")))
action_df = action_df.withColumn("data", F.from_json(F.col("data"), action_schema))
action_df.show(10)
action_df = action_df.withColumn("user_id", F.col("data.user_id"))
action_df = action_df.withColumn("item_id", F.col("data.item_id"))
action_df = action_df.select("user_id", "item_id")
action_df.printSchema()
action_df.show(10)

item_df = spark.read.format("text").option("multiLine", "false").text(item)
# filter empty row
item_df = item_df.filter("value is not null")

item_df = item_df.withColumn("data", extract_item(F.col("value")))
item_df = item_df.withColumn("item", F.from_json(F.col("data"), item_schema))

item_df = item_df.withColumn("id", F.col("item.item_id"))
item_df = item_df.withColumn("item_desc", F.col("item.item_desc"))
item_df.printSchema()
item_df.show(10)

combine_df = action_df.join(item_df, action_df["item_id"] == item_df["id"], "inner")
combine_df = combine_df.select("user_id", "item_id", "item_desc")

dyn_df = DynamicFrame.fromDF(combine_df, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(frame=dyn_df,
                                             connection_type="dynamodb",
                                             connection_options={"tableName": "demo-ohla-recommand"})

job.commit()
