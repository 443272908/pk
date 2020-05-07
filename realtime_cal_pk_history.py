from pyspark.sql import SparkSession, SQLContext, Window
import pyspark.sql.functions as func
import numpy as np
from pyspark.sql.types import StringType,TimestampType,FloatType,IntegerType,StructType,StructField,LongType
import os
from datetime import datetime
import pandas as pd
import requests
import time
# 初始化日志
import loginit
import logging
loginit.setup_logging('./logconfig.yml')


def get_pasr_realtime_value(spark_df, max_cal_period):
    """
    实时计算pasr的值 按照存储过程cd_11_sec.dbo.compute_mkt_pasr_v2翻译过来 未做改动
    :param spark_df: 行情数据 含 open high low close 为spark类型dataframe
    :param max_cal_period: 计算指标值需要多少根bar
    :return:
    """
    @func.pandas_udf("sec_cd string,var_cl string,mkt_cl string," \
                     "pub_dt timestamp," \
                     "pasr float,raw_pi string, f10 float", func.PandasUDFType.GROUPED_MAP)
    def get_pasr(group):
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
        group['raw_pi'] = pi_list
        group['f10'] = f10_list
        group = group[['sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'pasr', 'raw_pi', 'f10']]
        return group
    spark_df = spark_df.groupby('sec_cd').apply(get_pasr)
    # 得到股票的周期数
    w = Window.partitionBy('sec_cd')
    spark_df = spark_df.withColumn('sec_length', func.count('pub_dt').over(w))
    spark_df = spark_df.withColumn('cal_threshold', func.when(func.col('sec_length') < max_cal_period,
                                                           func.col('sec_length')-1).otherwise(max_cal_period - 1))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('row_no', func.count('pub_dt').over(w))
    spark_df = spark_df.filter(spark_df.row_no >= spark_df.cal_threshold)
    spark_df = spark_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'pasr', 'raw_pi','f10')
    return spark_df


# ema按储存过程翻译过来 与传统存在差异 可查看compute_mkt_adx_v2
def get_ema(v, period):
    for i in range(len(v)):
        if i == 0:
            ema_value = v[i]
        else:
            ema_value = 1/period*v[i]+(period-1)/period*ema_value
    return float(ema_value)


def get_dmi_realtime_value(spark_df, max_cal_period):
    """
    实时计算dmi的值 按照存储过程cd_11_sec.dbo.compute_mkt_adx_v2翻译过来 未做改动
    :param spark_df: 行情数据 含 open high low close 为spark类型dataframe
    :param max_cal_period: 计算指标值需要多少根bar
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
    # 得到股票的周期数
    w = Window.partitionBy('sec_cd')
    spark_df = spark_df.withColumn('sec_length', func.count('pub_dt').over(w))
    spark_df = spark_df.withColumn('cal_threshold', func.when(func.col('sec_length') < max_cal_period,
                                                           func.col('sec_length')-1).otherwise(max_cal_period - 1))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('row_no', func.count('pub_dt').over(w))
    spark_df = spark_df.filter(spark_df.row_no >= spark_df.cal_threshold)
    spark_df = spark_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'pdi', 'mdi')
    return spark_df


def get_amended_pasr(spark_df):
    """
    用pdi mdi的值对原始的pasr进行修正
    :param spark_df: 原始的pasr的值、原始的趋势及pdi,mdi的值 为spark类型dataframe
    :return:
    """
    @func.pandas_udf("sec_cd string,var_cl string,mkt_cl string," \
                     "pub_dt timestamp, pasr float,raw_pi string, f10 float," \
                     "pdi float,mdi float,pi string", func.PandasUDFType.GROUPED_MAP)
    def amend_pasr(group):
        group = group.sort_values('pub_dt')
        new_pi_list = group['raw_pi'].to_list()
        for i in range(1, len(group)):
            if new_pi_list[i-1] == 'Short':
                if group.iloc[i]['raw_pi'] == 'Short':
                    if group.iloc[i]['pdi'] - group.iloc[i]['mdi'] > 5:
                        new_pi_list[i] = 'NoTrade'
                    else:
                        new_pi_list[i] = 'Short'
                else:
                    if group.iloc[i]['pdi'] - group.iloc[i]['mdi'] > 5:
                        new_pi_list[i] = 'Long'
                    if group.iloc[i]['pdi'] - group.iloc[i]['mdi'] > 1:
                        new_pi_list[i] = 'NoTrade'
            elif new_pi_list[i-1] == 'Long':
                if group.iloc[i]['raw_pi'] == 'Long':
                    if group.iloc[i]['mdi'] - group.iloc[i]['pdi'] > 5:
                        new_pi_list[i] = 'NoTrade'
                    else:
                        new_pi_list[i] = 'Long'
                else:
                    if group.iloc[i]['mdi'] - group.iloc[i]['pdi'] > 5:
                        new_pi_list[i] = 'Short'
                    if group.iloc[i]['mdi'] - group.iloc[i]['pdi'] > 1:
                        new_pi_list[i] = 'NoTrade'
            else:
                if group.iloc[i]['pdi'] - group.iloc[i]['mdi'] > 5 and group.iloc[i]['raw_pi'] == 'Long':
                    new_pi_list[i] = 'Long'
                elif group.iloc[i]['mdi'] - group.iloc[i]['pdi'] > 5 and group.iloc[i]['raw_pi'] == 'Short':
                    new_pi_list[i] = 'Short'
                else:
                    new_pi_list[i] = 'NoTrade'
        group['pi'] = new_pi_list
        return group
    spark_df = spark_df.groupby('sec_cd').apply(amend_pasr)
    return spark_df


# 得到KDJ K值 D值
def get_k_d_value(v, n, m):
    return_value = 50
    if len(v) < n:
        return float(return_value)
    else:
        for i in range(n-1, len(v)):
            return_value = (m-1)/m*return_value + 1/m*v[i]
        return float(return_value)


def get_kdj_realtime_value(spark_df, max_cal_period):
    """
    实时计算kdj的值 具体参照matlab代码
    :param spark_df: 行情数据 含 open high low close 为spark类型dataframe
    :param max_cal_period: 计算指标值需要多少根bar
    :return:
    """
    kdj_n = 14  # 参数参照comput_qot_kdj
    kdj_m = 3
    kdj_l = 3
    kdj_s = 3
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('row_no', func.count('pub_dt').over(w))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt').rowsBetween(1 - kdj_n, Window.currentRow)
    spark_df = spark_df.withColumn('rsv', func.when(func.col('row_no') < kdj_n, 50)
                                   .otherwise((func.col('close') - func.min('low').over(w)) / (
                func.max('high').over(w) - func.min('low').over(w)) * 100))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    get_k_value = func.udf(lambda x: get_k_d_value(x, kdj_n, kdj_m), FloatType())
    spark_df = spark_df.withColumn("kdj_k", get_k_value(func.collect_list('rsv').over(w)))
    get_d_value = func.udf(lambda x: get_k_d_value(x, kdj_n, kdj_l), FloatType())
    spark_df = spark_df.withColumn("kdj_d", get_d_value(func.collect_list('kdj_k').over(w)))\
        .withColumn("kdj_j", kdj_s*func.col('kdj_k')-(kdj_s-1)*func.col('kdj_d'))
    # 得到股票的周期数
    w = Window.partitionBy('sec_cd')
    spark_df = spark_df.withColumn('sec_length', func.count('pub_dt').over(w))
    spark_df = spark_df.withColumn('cal_threshold', func.when(func.col('sec_length') < max_cal_period,
                                                           func.col('sec_length')-1).otherwise(max_cal_period - 1))
    spark_df = spark_df.filter(spark_df.row_no >= spark_df.cal_threshold)
    return spark_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'kdj_k', 'kdj_d')


def get_kdj_realtime_signal(spark_df):
    """
    基于kdj的值计算kdj信号
    :param spark_df: kdj的值 为spark类型dataframe
    :return:
    """
    @func.pandas_udf("sec_cd string,var_cl string,mkt_cl string," \
                     "pub_dt timestamp, kdj_k float,kdj_d float,kdj_s int", func.PandasUDFType.GROUPED_MAP)
    def get_kdj_signal(group):
        group = group.sort_values('pub_dt')
        kdj_s_list = [np.nan]*len(group)
        for i in range(len(group)):
            if group.iloc[i]['kdj_k'] >= 70:
                kdj_s_list[i] = 1
            elif group.iloc[i]['kdj_k'] <= 30:
                kdj_s_list[i] = 0
            else:
                if i == 0:
                    if group.iloc[i]['kdj_k'] >= group.iloc[i]['kdj_d']:
                        kdj_s_list[i] = 1
                    else:
                        kdj_s_list[i] = 0
                else:
                    if kdj_s_list[i-1] == 1:
                        if group.iloc[i]['kdj_k'] >= group.iloc[i]['kdj_d'] -2:
                            kdj_s_list[i] = 1
                        else:
                            kdj_s_list[i] = 0
                    else:
                        if group.iloc[i]['kdj_k'] > group.iloc[i]['kdj_d'] + 2:
                            kdj_s_list[i] = 1
                        else:
                            kdj_s_list[i] = 0
        group['kdj_s'] = kdj_s_list
        return group
    spark_df = spark_df.groupby('sec_cd').apply(get_kdj_signal)
    return spark_df


def get_realtime_data(max_window_period, every_time_stocks, period, is_debug = False):
    """
        从实时接口得到行情数据
    :param max_window_period: 获取的最大窗口
    :param every_time_stocks: 每次取的股票数
    :param period: 周期
    :return:
    """
    url = "https://quote.investoday.net/quote/codelist?code=stocklist"
    response = requests.get(url)
    response_dict = eval(response.text)
    data_dict = response_dict["ret"]
    stock_list = []
    for key, value in data_dict.items():
        if value[2] == 1:
            stock_list.append(key)
    stock_list = [stock for stock in stock_list if stock[:2] == '00' or stock[:2] == '30' or stock[:2] == '60']
    result_list = []

    for i in range(every_time_stocks, len(stock_list), every_time_stocks):
        tmp_stock_list = stock_list[i - every_time_stocks:i]
        url = "https://quote.investoday.net/quote/ex-klines?list="
        url += ','.join(tmp_stock_list) + "&period=" + str(period) + "&num=-" + str(max_window_period)
        while True:
            try:
                response = requests.get(url)
                break
            except Exception as e:
                logging.info('连接异常数据实时接口异常:' + str(e))
                time.sleep(3)
        null = ''
        response_dict = eval(response.text)
        data_dict = response_dict["ret"]
        ret_all_list = []
        for key, value in data_dict.items():
            ret_list = value['ret']
            if len(ret_list) == 0:
                logging.info(key + '没有行情数据')
                continue
            ret_list = [[key] + ret[:-1] for ret in ret_list]
            ret_all_list.extend(ret_list)
        result_list.extend(ret_all_list)
        logging.info('every' + str(every_time_stocks) + 'stock is ok!')
        if is_debug:
            break
    if not is_debug:
        tmp_stock_list = stock_list[i:]
        if len(tmp_stock_list) > 0:
            url = "https://quote.investoday.net/quote/ex-klines?list="
            url += ','.join(tmp_stock_list) + "&period=" + str(period) + "&num=-" + str(max_window_period)
            while True:
                try:
                    response = requests.get(url)
                    break
                except Exception as e:
                    logging.info('连接异常数据实时接口异常:' + str(e))
                    time.sleep(3)
            response_dict = eval(response.text)
            data_dict = response_dict["ret"]
            ret_all_list = []
            for key, value in data_dict.items():
                ret_list = value['ret']
                if len(ret_list) == 0:
                    logging.info(key + '没有行情数据')
                    continue
                ret_list = [[key] + ret[:-1] for ret in ret_list]
                ret_all_list.extend(ret_list)
            result_list.extend(ret_all_list)
    logging.info('all stock ok')
    col = ['sec_cd', 'timestamp', 'open', 'high', 'low', 'close', 'vol']
    result_df = pd.DataFrame(result_list, columns=col)
    result_df['timestamp'] = result_df['timestamp'].astype('str')
    return result_df


def dbwrite_mssql(df, host, user, password, table_name):
    """入库"""
    df.write.format('jdbc') \
        .option("url", "jdbc:sqlserver://%s;databaseName=CD_10_IND" % host) \
        .option("user", "%s" % user) \
        .option("password", "%s" % password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "%s" % table_name) \
        .mode("append") \
        .save()


if __name__ == "__main__":
    window_period = 300  # 往前拿多少根K线
    max_cal_period = 200  # 计算指标值需要多少根bar
    min_cal_period = 30  # 计算指标值最少需要多少根bar
    host = "192.168.12.111:1433"
    user = "pengs"
    password = "pengs0410"
    into_db_table_name = "IND_S_QOT_RT_PK"
    today = datetime.today().strftime('%Y-%m-%d')
    every_time_stocks = 500
    period = 'day'
    is_debug = True
    result_df = get_realtime_data(window_period, every_time_stocks, period, is_debug)
    spark = SparkSession \
        .builder \
        .appName("zh_cal_pk") \
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
    logging.info('成功从实时接口得到数据')
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    k_line_df = spark.createDataFrame(result_df, schema=schema)
    k_line_df = k_line_df.withColumn('var_cl', func.lit('A'))\
        .withColumn('mkt_cl', func.when(func.substring('sec_cd', 8, 9) == 'SZ', 'Z').otherwise('S'))
    udf_time_format = func.udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    k_line_df = k_line_df.withColumn('pub_dt', udf_time_format(func.substring('timestamp', 1, 10).cast(IntegerType())))
    # 过滤掉数据不足的股票
    w = Window.partitionBy('sec_cd')
    k_line_df = k_line_df.withColumn('sec_length', func.count('pub_dt').over(w))
    k_line_df = k_line_df.filter(k_line_df.sec_length >= min_cal_period)
    k_line_df = k_line_df.drop('sec_length', 'timestamp')
    # 得到修正pasr的值 及 修正的信号
    pasr_spark_df = get_pasr_realtime_value(k_line_df, max_cal_period)
    dmi_spark_df = get_dmi_realtime_value(k_line_df, max_cal_period)
    pasr_spark_df = pasr_spark_df.join(dmi_spark_df, on=['sec_cd', 'var_cl', 'mkt_cl','pub_dt'], how='inner')
    pasr_spark_df = get_amended_pasr(pasr_spark_df)
    # 得到kdj的值和信号
    kdj_spark_df = get_kdj_realtime_value(k_line_df, max_cal_period)
    kdj_spark_df = get_kdj_realtime_signal(kdj_spark_df)
    # 得到pasr的值 pasr信号 kdj的值 kdj的信号
    all_spark_df = pasr_spark_df.join(kdj_spark_df, on=['sec_cd', 'var_cl', 'mkt_cl', 'pub_dt'], how='inner')
    # 结合pasr 和 kdf 得出pk信号 无法确定的信号用-1代替
    all_spark_df = all_spark_df.withColumn('pk_sign', func.when((func.col('pi') == 'Long'), 1)\
        .when((((func.col('pi') == 'NoTrade') | (func.col('pi') == 'Short')) & (func.col('kdj_s') == 0)), 0)\
        .when((((func.col('pi') == 'NoTrade') | (func.col('pi') == 'Short')) & (func.col('kdj_s') == 1)), 1)\
        .otherwise(-1))
    # 对-1 用前一天的值填充
    def fill_forward(v):
        v = v[::-1]
        for i in range(len(v)):
            if v[i] != -1 or i == len(v)-1:
                return v[i]
    udf_fill_forward = func.udf(lambda x: float(fill_forward(x)), FloatType())
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    all_spark_df = all_spark_df.withColumn('pk_sign', func.when(func.col('pk_sign') != -1, func.col('pk_sign')) \
                                           .otherwise(udf_fill_forward(func.collect_list('pk_sign').over(w))))
    all_spark_df = all_spark_df.withColumn('period', func.lit(period)) \
        .withColumn('pub_dt', func.substring(func.col('pub_dt'), 1, 10)) \
        .withColumn('sec_cd', func.substring(func.col('sec_cd'), 1, 6))
    all_spark_df = all_spark_df.select('pub_dt', 'sec_cd', 'var_cl', 'mkt_cl', 'period',
                                   'pasr', 'pi', 'f10', 'pdi', 'mdi', 'raw_pi', 'kdj_k', 'kdj_d', 'kdj_s', 'pk_sign')
    dbwrite_mssql(all_spark_df, host, user, password, into_db_table_name)





