from datetime import datetime
from pyspark.sql import SparkSession,Window
import pyspark.sql.functions as func
from pyspark.sql.types import StringType,TimestampType,FloatType,IntegerType,StructType,StructField,LongType
import os
import platform
import numpy as np
import socket
# 初始化日志
import loginit
import logging
loginit.setup_logging('./logconfig.yml')


# 得到KDJ K值 D值
def get_k_d_value(v, n, m):
    return_value = 50
    if len(v) < n:
        return float(return_value)
    else:
        for i in range(n-1, len(v)):
            return_value = (m-1)/m*return_value + 1/m*v[i]
        return float(return_value)


# 得到KDJ值 具体代码参照matlab版本
def get_kdj_realtime_value(spark_df, cal_trend_ratio):
    kdj_n = 14  # 参数参照comput_qot_kdj
    kdj_m = 3
    kdj_l = 3
    kdj_s = 3
    w = Window.partitionBy('sec_cd').orderBy('pub_dt').rowsBetween(1 - kdj_n, Window.currentRow)
    spark_df = spark_df.withColumn('rsv', func.when(func.col('current_length') < kdj_n, 50)
                                   .otherwise((func.col('close') - func.min('low').over(w)) / (
                func.max('high').over(w) - func.min('low').over(w)) * 100))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    get_k_value = func.udf(lambda x: get_k_d_value(x, kdj_n, kdj_m), FloatType())
    spark_df = spark_df.withColumn("k_value", get_k_value(func.collect_list('rsv').over(w)))
    get_d_value = func.udf(lambda x: get_k_d_value(x, kdj_n, kdj_l), FloatType())
    spark_df = spark_df.withColumn("d_value", get_d_value(func.collect_list('k_value').over(w)))\
        .withColumn("j_value", kdj_s*func.col('k_value')-(kdj_s-1)*func.col('d_value'))
    # 取最后40%数据
    w = Window.partitionBy('sec_cd')
    spark_df = spark_df.withColumn('sec_length', func.count('pub_dt').over(w))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('row_no', func.count('pub_dt').over(w))
    spark_df = spark_df.filter(spark_df.row_no >= spark_df.sec_length * cal_trend_ratio)
    return spark_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'k_value', 'd_value')


# 基于kdj的值计算kdj信号
def get_kdj_realtime_signal(spark_df):
    @func.pandas_udf("sec_cd string,var_cl string,mkt_cl string," \
                     "pub_dt timestamp, k_value float,d_value float,kdj_s int", func.PandasUDFType.GROUPED_MAP)
    def get_kdj_signal(group):
        group = group.sort_values('pub_dt')
        kdj_s_list = [np.nan]*len(group)
        for i in range(len(group)):
            if group.iloc[i]['k_value'] >= 70:
                kdj_s_list[i] = 1
            elif group.iloc[i]['k_value'] <= 30:
                kdj_s_list[i] = 0
            else:
                if i == 0:
                    if group.iloc[i]['k_value'] >= group.iloc[i]['d_value']:
                        kdj_s_list[i] = 1
                    else:
                        kdj_s_list[i] = 0
                else:
                    if kdj_s_list[i-1] == 1:
                        if group.iloc[i]['k_value'] >= group.iloc[i]['d_value'] -2:
                            kdj_s_list[i] = 1
                        else:
                            kdj_s_list[i] = 0
                    else:
                        if group.iloc[i]['k_value'] > group.iloc[i]['d_value'] + 2:
                            kdj_s_list[i] = 1
                        else:
                            kdj_s_list[i] = 0
        group['kdj_s'] = kdj_s_list
        return group
    spark_df = spark_df.groupby('sec_cd').apply(get_kdj_signal)
    return spark_df


if __name__ == "__main__":
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
    logging.info('input csv_path is :'+str(csv_input_path))
    k_line_df = spark.read.csv(csv_input_path, header=True, schema=schema)
    k_line_df = k_line_df.withColumn('var_cl', func.lit('a'))\
        .withColumn('mkt_cl',func.when(func.substring('sec_cd',8, 9) == 'SZ', 'z').otherwise('s'))
    udf_time_format = func.udf(lambda x: datetime.fromtimestamp(x),TimestampType())
    k_line_df = k_line_df.withColumn('pub_dt', udf_time_format(func.substring('timestamp',1, 10).cast(IntegerType())))
    # 过滤掉数据不足以计算的股票
    w = Window.partitionBy('sec_cd')
    k_line_df = k_line_df.withColumn('sec_length',func.count('pub_dt').over(w))
    k_line_df = k_line_df.filter(k_line_df.sec_length >= min_cal_period)
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    k_line_df = k_line_df.withColumn('current_length', func.count('pub_dt').over(w))
    kdj_spark_df = get_kdj_realtime_value(k_line_df, cal_trend_ratio)
    kdj_spark_df = get_kdj_realtime_signal(kdj_spark_df)
    csv_output_path = 'file:///' + os.getcwd() + '/kdj_data.csv'
    kdj_spark_df.write.option('header', 'true').mode('overwrite').csv(csv_output_path)
    logging.info('output csv_path is :' + str(csv_output_path))