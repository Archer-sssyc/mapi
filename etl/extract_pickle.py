import pickle
import boto3
import sys

from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

file_path = "s3://example-data/ohla/filter_batch_result.pickle"
objects = list()


s3 = boto3.resource('s3',
                    region_name="cn-northwest-1")
bucket = s3.Bucket('example-data')
data = bucket.Object("ohla/filter_batch_result.pickle").get()['Body'].read()

u = pickle.loads(data)
for (k, v) in u.items():
    for (item_key, items) in v.items():
        entity = dict()
        entity["user_id"] = k
        entity["item_id"] = item_key
        entity["action"] = items[1]
        entity["ratio"] = float(items[2])
        entity["detail"] = items[3]
        objects.append(entity)

print(objects[0:10])

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
glue_spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame(objects)
df.printSchema()
df = df.select("user_id", "item_id", "ratio", "action", "detail")
df = df.filter(df["ratio"] > 0)
df.show(10)
dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(frame=dyn_df,
                                             connection_type="dynamodb",
                                             connection_options={"tableName": "demo-pickle-recommend"})

job.commit()
