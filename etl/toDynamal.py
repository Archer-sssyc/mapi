import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType

file_path_list = ["~/Work/NWCD/ohla/mapi/resource/filter_batch_result.pickle"]


@udf(returnType=StringType())
def extract_array(input_array):
    if input_array and isinstance(input_array, list) and len(input_array) == 1:
        return input_array[0]
    return input_array


@udf(returnType=StringType())
def extract_message(message_str):
    if message_str:
        t = message_str.index("url=")
        return message_str[t + 4:]
    return message_str


post_data_destinationAddress_schema = StructType([
    StructField("name", StringType()),
    StructField("line1", StringType()),
    StructField("line1", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("countryCode", StringType()),
    StructField("postalCode", StringType())]

)

items_schema = StructType([
    StructField("merchantSku", StringType()),
    StructField("asin", StringType()),
    StructField("fnSku", StringType()),
    StructField("quantity", IntegerType()),
    StructField("referenceItemId", StringType()),
])
post_data_schema = StructType([
    StructField("destinationAddress", post_data_destinationAddress_schema),
    StructField("items", ArrayType(items_schema)),
    StructField("shouldIncludeCOD", BooleanType()),
    StructField("shouldIncludeDeliveryWindows", BooleanType()),
    StructField("isBlankBoxRequired", BooleanType()),
    StructField("blockAMZL", BooleanType())
])

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
glue_spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = SparkSession.builder.getOrCreate()
es_df = spark.read.format("text").option("multiLine", "false").text(file_path_list)
es_df.printSchema()
dyn_df = DynamicFrame.fromDF(focus_df, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(frame=dyn_df, connection_type="dynamodb",
                                             connection_options={"tableName": "demo-recommended"})

job.commit()
