import sys
import os
import json
from pyspark.sql import functions as F

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType


action = "../resource/action.csv"
item = "../resource/item.csv"

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
combine_df.printSchema()
combine_df.show(10)
