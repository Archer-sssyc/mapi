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

                for (k, v) in u.items:
                    entity = dict()
                    entity["user_id"] = k
                    for (item_key, items) in v.items:
                        entity["item"] = item_key
                        entity["action"] = items[1]
                        entity["ratio"] = items[2]
                        entity["detail"] = items[3]
                    objects.append(entity)
            except EOFError:
                break
    print(objects)
    # spark = SparkSession.builder.appName("test").getOrCreate()
    # df = spark.createDataFrame(objects)
    # df.printSchema()
