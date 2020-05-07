from datetime import datetime
from pyspark.sql import SparkSession,Window
import pyspark.sql.functions as func
from pyspark.sql.types import StringType,TimestampType,FloatType,IntegerType,StructType,StructField,LongType
import os
import platform
import numpy as np
import socket


# ema按储存过程翻译过来 与传统存在差异 可查看compute_mkt_adx_v2
def get_ema(v, period):
    for i in range(len(v)):
        if i == 0:
            ema_value = v[i]
        else:
            ema_value = 1/period*v[i]+(period-1)/period*ema_value
    return float(ema_value)


def get_dmi_realtime_value(spark_df, cal_trend_ratio):
    """
    实时计算dmi的值 按照存储过程cd_11_sec.dbo.compute_mkt_adx_v2翻译过来 未做改动
    :param spark_df: 行情数据 含 open high low close 为spark类型dataframe
    :param cal_trend_ratio: 保留多少比例的数据用于计算信号
    :return:
    """
    dmi_period = 30  # 周期并非传统的14
    # 对于最后一行 x y z为空值
    udf_max = func.udf(lambda x, y, z: float(max(max(abs(x), abs(y)), abs(z))
                                             if x is not None and y is not None
                                                and z is not None else np.nan), FloatType())
    udf_get_ema = func.udf(lambda x: float(get_ema(x, dmi_period)), FloatType())
    # di_plus di_minus tr 与传统的算法存在差异 是用后一天减去当前天 (具体原因不明) 具体可查看compute_mkt_adx_v2
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('di_plus', func.lead('high', 1).over(w) - func.col('high'))\
        .withColumn('di_minus', func.col('low') - func.lead('low', 1).over(w))\
        .withColumn('tr', udf_max(
                            (func.lead('high', 1).over(w) - func.lead('low', 1).over(w)),
                            (func.lead('high', 1).over(w) - func.col('close')),
                            (func.col('close') - func.lead('low', 1).over(w))))
    # 将nan置为空
    spark_df = spark_df.replace(float('nan'), None)
    # 对di_plus di_minus 进行修正 sm_di_plus sm_di_minus sm_tr为di_plus di_minus tr的加权平均值
    # pdi mdi 此处取了前一天的值 不明白其中原因
    spark_df = spark_df\
        .withColumn('di_plus', func.when(
        (func.col('di_plus') > func.col('di_minus')) & (func.col('di_plus') > 0),
        func.col('di_plus')).otherwise(0))\
        .withColumn('di_minus', func.when(
        (func.col('di_minus') > func.col('di_plus')) & (func.col('di_minus') > 0),
        func.col('di_minus')).otherwise(0))\
        .withColumn('sm_di_plus', udf_get_ema(func.collect_list('di_plus').over(w)))\
        .withColumn('sm_di_minus', udf_get_ema(func.collect_list('di_minus').over(w)))\
        .withColumn('sm_tr', udf_get_ema(func.collect_list('tr').over(w)))\
        .withColumn('pdi', func.col('sm_di_plus')/func.col('sm_tr')*100)\
        .withColumn('mdi', func.col('sm_di_minus')/func.col('sm_tr')*100)\
        .withColumn('pdi', func.lag('pdi', 1).over(w))\
        .withColumn('mdi', func.lag('mdi', 1).over(w))
    # 取最后40%数据
    # w = Window.partitionBy('sec_cd')
    # spark_df = spark_df.withColumn('sec_length', func.count('pub_dt').over(w))
    # w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    # spark_df = spark_df.withColumn('row_no', func.count('pub_dt').over(w))
    # spark_df = spark_df.filter(spark_df.row_no >= spark_df.sec_length * cal_trend_ratio)
    # spark_df = spark_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'pdi', 'mdi')
    return spark_df


min_cal_period = 60  # 最少需要60根bar
cal_trend_ratio = 0.6  # 计算指标的数据、计算趋势的数据比例分配
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
csv_input_path = 'file:///' + os.getcwd() + '/realtime_data.csv'

k_line_df = spark.read.csv(csv_input_path, header=True, schema=schema)
k_line_df = k_line_df.filter(k_line_df.sec_cd == '000502.SZ')
k_line_df = k_line_df.withColumn('var_cl', func.lit('a')) \
    .withColumn('mkt_cl', func.when(func.substring('sec_cd', 8, 9) == 'SZ', 'z').otherwise('s'))
udf_time_format = func.udf(lambda x: datetime.fromtimestamp(x), TimestampType())
k_line_df = k_line_df.withColumn('pub_dt', udf_time_format(func.substring('timestamp', 1, 10).cast(IntegerType())))
dmi_spark_df = get_dmi_realtime_value(k_line_df, cal_trend_ratio)
dmi_spark_df.show(1000)