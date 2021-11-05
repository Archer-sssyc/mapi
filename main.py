from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pickle
import pandas as pd

if __name__ == '__main__':
    objects = list()
    file_path = "/Users/fugui/Work/NWCD/ohla/mapi/resource/filter_batch_result.pickle"
    with open(file_path, "rb") as openfile:
        while True:
            try:
                u = pickle.load(openfile)
                for (k, v) in u.items():
                    for (item_key, items) in v.items():
                        entity = dict()
                        entity["user_id"] = k
                        entity["item_id"] = item_key
                        entity["action"] = items[1]
                        entity["ratio"] = float(items[2])
                        entity["detail"] = items[3]
                        objects.append(entity)
            except EOFError:
                break

    t = [item for item in objects if item["ratio"] > 0]
    print(t[0:10])
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.createDataFrame(objects)
    df.printSchema()
    df = df.select("user_id", "item_id", "ratio", "action", "detail")
    df = df.filter(df["ratio"] > 0)
    df.show(10)
