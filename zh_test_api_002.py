from datetime import datetime
from pyspark.sql import SparkSession,Window
import pyspark.sql.functions as func
from pyspark.sql.types import StringType,TimestampType,FloatType,IntegerType,StructType,StructField,LongType
import os
import platform
# 初始化日志
import loginit
import logging
loginit.setup_logging('./logconfig.yml')

plat_tuple = platform.architecture()
system = platform.system()
plat_version = platform.platform()
if system == 'Windows':
    logging.info('this is windows system')
    logging.info('version is: ' + plat_version)
elif system == 'Linux':
    logging.info('this is linux system ')
    logging.info('version is: ' + plat_version)
    os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3'


spark = SparkSession \
    .builder \
    .appName("zh_cal_pk") \
    .master("local[*]") \
    .getOrCreate()
schema = StructType([
    StructField('sec_cd', StringType(), True),
    StructField('timestamp', StringType(), True),
    StructField('open', FloatType(), True),
    StructField('high', FloatType(), True),
    StructField('low', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('vol', FloatType(), True),
])
result_df = spark.read.csv("realtime_data.csv",header=True,schema=schema)
udf_time_format = func.udf(lambda x: datetime.fromtimestamp(x),TimestampType())
result_df = result_df.withColumn('date', udf_time_format(func.substring('timestamp',1, 10).cast(IntegerType())))
w = Window.partitionBy('sec_cd')
result_df = result_df.withColumn('count',func.count('date').over(w))
result_df = result_df.filter(result_df.count >= 60)
result_df.show(1000)