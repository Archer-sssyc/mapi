import pickle
import boto3
from pyspark.sql import SparkSession

file_path = "s3://example-data/ohla/filter_batch_result.pickle"
objects = list()

s3 = boto3.resource('s3',
                    aws_access_key_id="AKIAQMS6D5EI3FOTFUE4",
                    aws_secret_access_key="your key",
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

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame(objects)
df.printSchema()
df = df.select("user_id", "item_id", "ratio", "action", "detail")
df = df.filter(df["ratio"] > 0)
df.show(10)
