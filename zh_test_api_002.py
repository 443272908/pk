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


plat_tuple = platform.architecture()
system = platform.system()
plat_version = platform.platform()
if system == 'Windows':
    logging.info('this is windows system')
    logging.info('version is: ' + plat_version)
elif system == 'Linux':
    logging.info('this is linux system ')
    logging.info('version is: ' + plat_version)
    host_name = socket.gethostname()
    if 'zhanghang' in host_name:
        os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3'
        os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3'
        os.environ.setdefault('ARROW_PRE_0_15_IPC_FORMAT', '1')


def get_psar_realtime_signal(spark_df, cal_trend_ratio):
    @func.pandas_udf("sec_cd string,var_cl string,mkt_cl string," \
                     "pub_dt timestamp," \
                     "pasr float,pi string, f10 float", func.PandasUDFType.GROUPED_MAP)
    def get_sar(group):
        group = group.sort_values('pub_dt')
        sar_t = group['low'].iloc[:5].min()
        ep_t = group['high'].iloc[:5].max()
        ep_sar_t = sar_t - ep_t
        tempVal_h = group.iloc[4]['high']
        tempVal_l = group.iloc[4]['low']
        p_t1 = 'Long'
        if p_t1 == 'Long':
            if tempVal_l > sar_t:
                p_t = 'Long'
            else:
                p_t = 'Short'
        else:
            if tempVal_h < sar_t:
                p_t = 'Short'
            else:
                p_t = 'Long'
        if p_t != p_t1:
            af_t = 0.02
            afd_t = af_t * ep_sar_t
        else:
            af_t = np.nan
            afd_t = np.nan
        step = 0.02
        z = 0.2
        y = 0.02
        is_updt = 0
        n = len(group)
        pasr_list = [np.nan] * n
        pi_list = [''] * n
        f10_list = [np.nan] * n
        pasr_list[4] = sar_t
        pi_list[4] = p_t
        for i in range(5, n):
            sar_t1 = sar_t
            p_t1 = p_t
            ep_t1 = ep_t
            afd_t1 = afd_t
            af_t1 = af_t
            ep_sar_t1 = ep_sar_t
            l_t = group.iloc[i]['low']
            h_t = group.iloc[i]['high']
            if p_t1 == 'Long' and sar_t1 + afd_t1 < l_t:
                sar_t = sar_t1 + afd_t1
                if h_t > ep_t1:
                    ep_t = h_t
                else:
                    ep_t = ep_t1
                is_updt = 1
                if ep_t > h_t:
                    af_t = af_t1
                elif af_t1 + step < z:
                    af_t = af_t1 + step
                else:
                    af_t = z
            if p_t1 == 'Long' and is_updt == 0:
                p_t = 'Short'
                ep_t = l_t
                sar_t = ep_t1
                af_t = y
                is_updt = 1
            if is_updt == 0 and sar_t1 - afd_t1 > h_t:
                p_t = 'Short'
                sar_t = sar_t1 - afd_t1
                if l_t < ep_t1:
                    ep_t = l_t
                else:
                    ep_t = ep_t1
                if ep_t < l_t:
                    af_t = af_t1
                elif af_t1 + step < z:
                    af_t = af_t1 + step
                else:
                    af_t = z
                is_updt = 1
            if is_updt == 0:
                p_t = 'Long'
                sar_t = ep_t1
                ep_t = h_t
                af_t = y
                is_udpt = 1
            ep_sar_t = abs(ep_t - sar_t)
            afd_t = ep_sar_t * af_t
            is_updt = 0
            pasr_list[i] = sar_t
            pi_list[i] = p_t
            f10_list[i] = afd_t
        group['pasr'] = pasr_list
        group['pi'] = pi_list
        group['f10'] = f10_list
        group = group[['sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'pasr', 'pi', 'f10']]
        return group
    spark_df = spark_df.groupby('sec_cd').apply(get_sar)
    # 取最后40%数据
    w = Window.partitionBy('sec_cd')
    spark_df = spark_df.withColumn('sec_length', func.count('pub_dt').over(w))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('row_no', func.count('pub_dt').over(w))
    spark_df = spark_df.filter(spark_df.row_no >= spark_df.sec_length * cal_trend_ratio)
    spark_df = spark_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'pasr', 'pi','f10')
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
    w = Window.partitionBy('sec_cd')
    k_line_df = k_line_df.withColumn('sec_length',func.count('pub_dt').over(w))
    k_line_df = k_line_df.filter(k_line_df.sec_length >= min_cal_period)
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    k_line_df = k_line_df.withColumn('current_length', func.count('pub_dt').over(w))
    psar_spark_df = get_psar_realtime_signal(k_line_df, cal_trend_ratio)
    csv_output_path = 'file:///' + os.getcwd() + '/pasr_data.csv'
    psar_spark_df.write.option('header','true').mode('overwrite').csv(csv_output_path)
    # psar_spark_df.show()
    # csv_path = os.getcwd()+'/pk_signal.csv'
    # psar_spark_df.write.option('header', 'true').mode('overwrite').csv(csv_path)
    # psar_spark_df.write.option('header', 'true').mode('overwrite').csv("file:///tmp/a_zhangh/pk_signal.csv")
