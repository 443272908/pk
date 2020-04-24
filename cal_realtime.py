# -*- coding:utf-8 -*-
from datetime import datetime, timedelta
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import pyspark.sql.functions as func
import requests
import time
import logging

csv_path = 'file:///Users/roger/data/temp/calrealtime.csv'


# 连接数据库
def dbquery(sqlContext, sql):
    data = sqlContext.read.format('jdbc') \
        .option("url", "jdbc:sqlserver://192.168.12.111:1433") \
        .option("user", "pengs") \
        .option("password", "pengs0410") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("query", sql) \
        .load()
    return data


# 得到MA信号
def get_ma_realtime_signal(spark_df, date_time):
    period_ma5 = 5
    period_ma10 = 10
    period_ma20 = 20
    period_ma60 = 60
    period_ma120 = 120
    period_ma250 = 250
    window_ma5 = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - period_ma5, Window.currentRow)
    window_ma10 = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - period_ma10, Window.currentRow)
    window_ma20 = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - period_ma20, Window.currentRow)
    window_ma60 = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - period_ma60, Window.currentRow)
    window_ma120 = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - period_ma120, Window.currentRow)
    window_ma250 = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - period_ma250, Window.currentRow)
    # 计算Ma
    spark_df = spark_df.withColumn('ma5', func.round(func.avg('close').over(window_ma5),4)) \
        .withColumn('ma10', func.round(func.avg('close').over(window_ma10),4)) \
        .withColumn('ma20', func.round(func.avg('close').over(window_ma20),4)) \
        .withColumn('ma60', func.round(func.avg('close').over(window_ma60),4)) \
        .withColumn('ma120', func.round(func.avg('close').over(window_ma120),4)) \
        .withColumn('ma250', func.round(func.avg('close').over(window_ma250),4))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn("prev_close", func.lag('close', 1).over(w))\
        .withColumn("prev_ma5", func.lag('ma5', 1).over(w)) \
        .withColumn("prev_ma20", func.lag('ma20', 1).over(w)) \
        .withColumn("prev_ma250", func.lag('ma250', 1).over(w))
    spark_df = spark_df.withColumn("ma_status_1", func.when((func.col("close") > func.col("ma5"))
                                                              & (func.col("ma5") > func.col("ma10"))
                                                              & (func.col("ma10") > func.col("ma20"))
                                                              & (func.col("ma20") > func.col("ma60"))
                                                              & (func.col("close") > func.col("open"))
                                                              & (func.col("length") > period_ma60)
                                                              , 1).when((func.col("close") < func.col("ma5"))
                                                              & (func.col("ma5") < func.col("ma10"))
                                                              & (func.col("ma10") < func.col("ma20"))
                                                              & (func.col("ma20") < func.col("ma60"))
                                                              & (func.col("close") < func.col("open"))
                                                              & (func.col("length") > period_ma60)
                                                              , -1).otherwise(0))
    spark_df = spark_df.withColumn("ma_status_3", func.when((func.col("prev_ma5") < func.col("prev_ma20"))
                                                              & (func.col("ma5") > func.col("ma20"))
                                                              & (func.col("length") > period_ma20)
                                                              , 1).when((func.col("prev_ma5") > func.col("prev_ma20"))
                                                              & (func.col("ma5") < func.col("ma20"))
                                                              & (func.col("length") > period_ma20)
                                                              , -1).otherwise(0))
    spark_df = spark_df.withColumn("ma_status_4", func.when((func.col("prev_close") < func.col("prev_ma250"))
                                                              & (func.col("close") > func.col("ma250"))
                                                              & (func.col("length") > period_ma250)
                                                              , 1).when((func.col("prev_close") > func.col("prev_ma250"))
                                                              & (func.col("close") < func.col("ma250"))
                                                              & (func.col("length") > period_ma250)
                                                              , -1).otherwise(0))
    return spark_df[spark_df.pub_dt == date_time].select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'length', 'close', 'ma5', 'ma10', 'ma20', 'ma60',
                                                         'ma120', 'ma250', 'ma_status_1', 'ma_status_3', 'ma_status_4')


# 得到EMA
def get_ema(v, period):
    k = 2 / (period + 1)
    ema_value = 0
    for i in range(len(v)):
        if i == 0:
            ema_value = v[i]
        else:
            ema_value = v[i]*k+ema_value*(1-k)
    return float(ema_value)


# 得到MACD信号
def get_macd_realtime_signal(spark_df, date_time):
    period_ma5 = 5
    macd_fast_period = 12
    macd_slow_period = 26
    macd_dea_period = 9
    macd_min_signal_length = 40  # 最小信号长度
    window_ma5 = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - period_ma5, Window.currentRow)
    spark_df = spark_df.withColumn('close_ma5', func.avg('close').over(window_ma5))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('close_list', func.collect_list('close').over(w))
    macd_fast_ema = func.udf(lambda x: get_ema(x, macd_fast_period), FloatType())
    spark_df = spark_df.withColumn("macd_fast_ema",macd_fast_ema('close_list'))
    macd_slow_ema = func.udf(lambda x: get_ema(x, macd_slow_period), FloatType())
    spark_df = spark_df.withColumn("macd_slow_ema", macd_slow_ema('close_list'))
    spark_df = spark_df.withColumn("macd_diff", func.col("macd_fast_ema") - func.col("macd_slow_ema"))\
        .withColumn("macd_diff_list", func.collect_list(func.col("macd_diff")).over(w))
    macd_diff_ema = func.udf(lambda x: get_ema(x, macd_dea_period), FloatType())
    spark_df = spark_df.withColumn('macd_dea',macd_diff_ema("macd_diff_list"))\
        .withColumn("macd_value", (func.col("macd_diff")-func.col("macd_dea"))*2)\
        .withColumn("prev_macd_diff", func.lag('macd_diff',1).over(w))\
        .withColumn("prev_macd_dea", func.lag('macd_dea', 1).over(w))\
        .withColumn("prev_close_ma5", func.lag('close_ma5', 1).over(w))\
        .withColumn("prev_1_macd_value", func.lag('macd_value', 1).over(w))\
        .withColumn("prev_2_macd_value", func.lag('macd_value', 2).over(w))\
        .withColumn("prev_3_macd_value", func.lag('macd_value', 3).over(w))\
        .withColumn("prev_4_macd_value", func.lag('macd_value', 4).over(w))
    spark_df = spark_df.withColumn("macd_status_4", func.when((func.col("prev_macd_diff") < func.col("prev_macd_dea"))
                                                    & (func.col("macd_diff") > func.col("macd_dea"))
                                                    & (func.col("length") > macd_min_signal_length)
                                                    , 1).when((func.col("prev_macd_diff") > func.col("prev_macd_dea"))
                                                    & (func.col("macd_diff") < func.col("macd_dea"))
                                                    & (func.col("length") > macd_min_signal_length)
                                                    , -1).otherwise(0))
    spark_df = spark_df.withColumn("macd_status_3", func.when(
                                                    (func.col("prev_1_macd_value") > 0)
                                                    & (func.col("macd_value") > func.col("prev_1_macd_value"))
                                                    & (func.col("prev_2_macd_value") > func.col("prev_1_macd_value"))
                                                    & (func.col("prev_3_macd_value") > func.col("prev_2_macd_value"))
                                                    & (func.col("prev_4_macd_value") > func.col("prev_3_macd_value"))
                                                    & (func.col("close_ma5") > func.col("prev_close_ma5"))
                                                    & (func.col("length") > macd_min_signal_length)
                                                    , 1).otherwise(0))
    return spark_df[spark_df.pub_dt == date_time].select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'macd_diff', 'macd_dea',
                                                         'macd_value', 'macd_status_3', 'macd_status_4')


# 得到KDJ K值 D值
def get_k_d_value(v, n, m):
    return_value = 50
    if len(v) < n:
        return float(return_value)
    else:
        for i in range(n-1, len(v)):
            return_value = (m-1)/m*return_value + 1/m*v[i]
        return float(return_value)


# 得到KDJ信号
def get_kdj_realtime_signal(spark_df, date_time):
    kdj_n = 9
    kdj_m = 3
    kdj_l = 3
    kdj_s = 3
    kdj_min_signal_length = 20
    w = Window.partitionBy('sec_cd').orderBy('pub_dt').rowsBetween(1 - kdj_n, Window.currentRow)
    spark_df = spark_df.withColumn('rsv', func.when(func.col('current_length') < kdj_n, 50).otherwise((func.col('close') - func.min('low').over(w)) /(
                func.max('high').over(w) - func.min('low').over(w)) * 100))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('rsv_list', func.collect_list('rsv').over(w))
    get_k_value = func.udf(lambda x: get_k_d_value(x, kdj_n, kdj_m), FloatType())
    spark_df = spark_df.withColumn("k_value", get_k_value('rsv_list'))\
        .withColumn('k_value_list', func.collect_list('k_value').over(w))
    get_d_value = func.udf(lambda x: get_k_d_value(x, kdj_n, kdj_l), FloatType())
    spark_df = spark_df.withColumn("d_value", get_d_value('k_value_list'))\
        .withColumn("j_value", kdj_s*func.col('k_value')-(kdj_s-1)*func.col('d_value'))\
        .withColumn("prev_j_value", func.lag('j_value', 1).over(w))\
        .withColumn("prev_d_value", func.lag('d_value', 1).over(w))
    spark_df = spark_df.withColumn("kdj_status_1", func.when((func.col("prev_j_value") < func.col("prev_d_value"))
                                                    & (func.col("j_value") > func.col("d_value"))
                                                    & (func.col("length") > kdj_min_signal_length)
                                                    , 1).when((func.col("prev_j_value") > func.col("prev_d_value"))
                                                    & (func.col("j_value") < func.col("d_value"))
                                                    & (func.col("length") > kdj_min_signal_length)
                                                    , -1).otherwise(0))
    return spark_df[spark_df.pub_dt == date_time].select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'k_value', 'd_value','j_value','kdj_status_1')


# 得到RSI信号
def get_rsi_realtime_signal(spark_df, date_time):
    rsi_period_1 = 14
    rsi_period_2 = 5
    rsi_min_signal_length = 15
    w = Window.orderBy('pub_dt').partitionBy('sec_cd')
    spark_df = spark_df.withColumn("diff_close", func.col('close')-func.lag("close", 1).over(w))
    w = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - rsi_period_1, Window.currentRow)
    spark_df = spark_df.withColumn("diff_close_sum", func.sum(func.when(func.col("diff_close") > 0, func.col("diff_close")).otherwise(0)).over(w))\
        .withColumn("diff_close_abs_sum", func.sum(func.abs(func.col("diff_close"))).over(w))\
        .withColumn("rsi_slow", func.when(func.col('current_length') <= rsi_period_1, 50).otherwise(func.round(func.col("diff_close_sum")/func.col("diff_close_abs_sum")*100,4)))
    w = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - rsi_period_2, Window.currentRow)
    spark_df = spark_df.withColumn("diff_close_sum", func.sum(func.when(func.col("diff_close") > 0, func.col("diff_close")).otherwise(0)).over(w))\
        .withColumn("diff_close_abs_sum", func.sum(func.abs(func.col("diff_close"))).over(w))\
        .withColumn("rsi_fast", func.when(func.col('current_length') <= rsi_period_2, 50).otherwise(func.round(func.col("diff_close_sum")/func.col("diff_close_abs_sum")*100,4)))
    w = Window.orderBy('pub_dt').partitionBy('sec_cd')
    spark_df = spark_df.withColumn("prev_rsi_slow", func.lag('rsi_slow', 1).over(w))\
        .withColumn("prev_rsi_fast", func.lag('rsi_fast', 1).over(w))
    spark_df = spark_df.withColumn("rsi_status", func.when((func.col("prev_rsi_fast") < func.col("prev_rsi_slow"))
                                                    & (func.col("rsi_fast") > func.col("rsi_slow"))
                                                    & (func.col("length") > rsi_min_signal_length)
                                                    , 1).when((func.col("prev_rsi_fast") > func.col("prev_rsi_slow"))
                                                    & (func.col("rsi_fast") < func.col("rsi_slow"))
                                                    & (func.col("length") > rsi_min_signal_length)
                                                    , -1).otherwise(0))
    return spark_df[spark_df.pub_dt == date_time].select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'rsi_slow', 'rsi_fast', 'rsi_status')


# 得到BOLL信号
def get_boll_realtime_signal(spark_df, date_time):
    boll_period = 26
    boll_width = 2
    window_ma = Window.orderBy('pub_dt').partitionBy('sec_cd').rowsBetween(1 - boll_period, Window.currentRow)
    spark_df = spark_df.withColumn('middle_line', func.avg('close').over(window_ma))\
        .withColumn('std', func.stddev('close').over(window_ma))\
        .withColumn('upper_line', func.col('middle_line') + boll_width*func.col('std'))\
        .withColumn('lower_line', func.col('middle_line') - boll_width*func.col('std'))
    w = Window.orderBy('pub_dt').partitionBy('sec_cd')
    spark_df = spark_df.withColumn("prev_close", func.lag('close', 1).over(w))\
        .withColumn("prev_upper_line", func.lag('upper_line', 1).over(w))\
        .withColumn("prev_lower_line", func.lag('lower_line', 1).over(w))
    spark_df = spark_df.withColumn("boll_status_1", func.when((func.col("prev_close") < func.col("prev_upper_line"))
                                                    & (func.col("close") > func.col("upper_line"))
                                                    & (func.col("length") > boll_period)
                                                    , 1).when((func.col("prev_close") > func.col("prev_upper_line"))
                                                    & (func.col("close") < func.col("upper_line"))
                                                    & (func.col("length") > boll_period)
                                                    , -1).otherwise(0))
    spark_df = spark_df.withColumn("boll_status_3", func.when((func.col("prev_close") < func.col("prev_lower_line"))
                                                    & (func.col("close") > func.col("lower_line"))
                                                    & (func.col("length") > boll_period)
                                                    , 1).when((func.col("prev_close") > func.col("prev_lower_line"))
                                                    & (func.col("close") < func.col("lower_line"))
                                                    & (func.col("length") > boll_period)
                                                    , -1).otherwise(0))
    return spark_df[spark_df.pub_dt == date_time].select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'middle_line',
                                                         'upper_line', 'lower_line', 'boll_status_1', 'boll_status_3')


# 得到实时数据
def get_realtime_data(period):
    try:
        url = "https://quote.investoday.net/quote/rthq-ext?code=stocklist&adj=-" + str(period)
        response = requests.get(url)
        response_dict = eval(response.text)
        data = response_dict["ret"]
    except Exception as e:
        print(str(url)+"异常:"+str(e))
        return None
    return data


# 得到所有的信号
def get_all_signal(input_df, date_time):
    input_df = input_df.withColumn("open", input_df["open"].cast(FloatType())) \
        .withColumn("high", input_df["high"].cast(FloatType())) \
        .withColumn("low", input_df["low"].cast(FloatType())) \
        .withColumn("close", input_df["close"].cast(FloatType()))
    w = Window.partitionBy('sec_cd')
    input_df = input_df.withColumn('length', func.count('pub_dt').over(w))\
        .withColumn('max_pub_dt', func.max('pub_dt').over(w))
    input_df = input_df[(input_df.length > 5) & (input_df.max_pub_dt == date_time)]
    w = Window.partitionBy('sec_cd').orderBy('pub_dt').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    input_df = input_df.withColumn('current_length', func.count('pub_dt').over(w))
    ma_spark_df = get_ma_realtime_signal(input_df, date_time)
    macd_spark_df = get_macd_realtime_signal(input_df, date_time)
    kdj_spark_df = get_kdj_realtime_signal(input_df, date_time)
    rsi_spark_df = get_rsi_realtime_signal(input_df, date_time)
    boll_spark_df = get_boll_realtime_signal(input_df, date_time)
    signal_df = ma_spark_df.join(macd_spark_df, on=["sec_cd", "var_cl", "mkt_cl", "pub_dt"], how="inner") \
        .join(kdj_spark_df, on=["sec_cd", "var_cl", "mkt_cl", "pub_dt"], how="inner") \
        .join(rsi_spark_df, on=["sec_cd", "var_cl", "mkt_cl", "pub_dt"], how="inner") \
        .join(boll_spark_df, on=["sec_cd", "var_cl", "mkt_cl", "pub_dt"], how="inner")
    return signal_df


def db_init(spark, sqlContext, is_debug):
    window_period = 260
    adjust_period = 5
    begin_date = (datetime.today() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")  # 三年前
    today = datetime.now().strftime("%Y-%m-%d")  # 今天
    sql = """
        select sec_cd,var_cl,mkt_cl,pub_dt,open_p 'open',high,low,close_p 'close' from
        (select sec_cd,var_cl,mkt_cl,pub_dt,F0020 'open_p',F0030 'high',F0040 'low',F0050 'close_p',ROW_NUMBER() over (partition by sec_cd,var_cl,mkt_cl order by pub_dt desc) as row_no 
        from cd_10_sec.dbo.QOT_D_BCK with(nolock) where var_cl = 'a' and is_vld = 1 and mkt_cl in ('s','z') and F0060 > 0) as t1
        where row_no <= %d and pub_dt >= '%s' and pub_dt < '%s'
        """ % (window_period, begin_date, today)
    if is_debug:
        sql += " and sec_cd in ('000002','000903','300033','300627','600600') "
    logging.info('sql:%s', sql)
    k_line_df = dbquery(sqlContext, sql)
    return k_line_df

def run(spark, data, k_line_df, today, is_debug, csv_path):
    if data is not None:
        time_stamp = int(str(data[0][-3])[:10])
        all_date_time = datetime.fromtimestamp(time_stamp)
        date_str = all_date_time.strftime("%Y-%m-%d %H:%M:%S")
        if today == date_str[:10]:
            date_time = datetime.strptime(date_str[:10], "%Y-%m-%d")
            data = [the_data[:6] + the_data[11:13] for the_data in data]
            col = ['sec_cd', 'open', 'pre_close', 'close', 'high', 'low', 'adjust_high', 'adjust_low']
            today_df = spark.createDataFrame(data, col)
            today_df = today_df.withColumn('pub_dt', func.lit(date_time)) \
                .withColumn('mkt_cl', func.when(func.col('sec_cd').substr(8, 9) == 'SZ', 'Z').otherwise('S')) \
                .withColumn('var_cl', func.lit('A')).withColumn('sec_cd', func.col('sec_cd').substr(1, 6))
            today_df = today_df[(today_df.close != 0) & (today_df.open != 0)]
            # 已经收盘
            if date_str[11:] >= "15:00:00" or "13:00:00" >= date_str[11:] >= "11:30:00":
                today_df = today_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'open', 'high', 'low', 'close')
                if is_debug:
                    today_df = today_df[today_df.sec_cd.isin(['000002', '000903', '300033', '300627', '600600'])]
                k_line_df = k_line_df.unionAll(today_df)
                signal_df = get_all_signal(k_line_df, date_time)
            else:
                # 以前一个周期(5分钟、10分钟等)的最高价和最低价分别作为收盘价算一次信号，以免信号遗漏
                today_df_1 = today_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'open', 'high', 'low',
                                             'adjust_high').withColumnRenamed('adjust_high', 'close')
                today_df_2 = today_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'open', 'high', 'low',
                                             'adjust_low').withColumnRenamed('adjust_low', 'close')
                if is_debug:
                    today_df_1 = today_df_1[today_df_1.sec_cd.isin(['000002', '000903', '300033', '300627', '600600'])]
                    today_df_2 = today_df_2[today_df_2.sec_cd.isin(['000002', '000903', '300033', '300627', '600600'])]
                k_line_df_1 = k_line_df.unionAll(today_df_1)
                k_line_df_2 = k_line_df.unionAll(today_df_1)
                signal_df_1 = get_all_signal(k_line_df_1, date_time)
                signal_df_2 = get_all_signal(k_line_df_2, date_time)
                status_list = ['ma_status_1', 'ma_status_3', 'ma_status_4', 'macd_status_3', 'macd_status_4',
                               'kdj_status_1', 'rsi_status', 'boll_status_1', 'boll_status_3']
                value_list = ['length', 'close', 'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'ma250', 'macd_diff',
                              'macd_dea', 'macd_value', 'k_value',
                              'd_value', 'j_value', 'rsi_slow', 'rsi_fast', 'middle_line', 'upper_line', 'lower_line']
                for value in value_list:
                    signal_df_1 = signal_df_1.withColumnRenamed(value, value + '_high')
                    signal_df_2 = signal_df_2.withColumnRenamed(value, value + '_low')
                for status in status_list:
                    signal_df_1 = signal_df_1.withColumnRenamed(status, status + '_high')
                    signal_df_2 = signal_df_2.withColumnRenamed(status, status + '_low')
                signal_df = signal_df_1.join(signal_df_2, on=["sec_cd", "var_cl", "mkt_cl", "pub_dt"], how="inner")
                for status in status_list:
                    signal_df = signal_df.withColumn(status, func.when(func.col(status + "_high") != 0,
                                                                       func.col(status + "_high")) \
                                                     .when(func.col(status + "_low") != 0,
                                                           func.col(status + "_low")).otherwise(0))
            signal_df = signal_df.withColumn('pub_dt', func.lit(all_date_time))  # 精确到秒
            signal_df.coalesce(1).write.option('header', 'true').mode('overwrite').csv(
                csv_path)
            signal_df = signal_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'ma_status_1', 'ma_status_3',
                                         'ma_status_4',
                                         'macd_status_3', 'macd_status_4', 'kdj_status_1', 'rsi_status',
                                         'boll_status_1', 'boll_status_3')
            # 过滤没有触发信号的记录
            signal_df = signal_df.filter(func.abs(func.col('ma_status_1')) + func.abs(func.col('ma_status_3')) +
                                         func.abs(func.col('ma_status_4')) + func.abs(func.col('macd_status_3')) +
                                         func.abs(func.col('macd_status_4')) + func.abs(func.col('kdj_status_1')) +
                                         func.abs(func.col('rsi_status')) + func.abs(func.col('boll_status_1')) +
                                         func.abs(func.col('boll_status_3')) > 0)
            return signal_df


if __name__ == "__main__":
    ss = SparkSession \
        .builder \
        .appName("macd") \
        .master("local[*]") \
        .getOrCreate()
    sqlContext = SQLContext(ss)

    is_debug = True
    adjust_period = 5
    begin_date = (datetime.today() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")  # 三年前
    today = datetime.now().strftime("%Y-%m-%d")  # 今天

    # 获取历史数据
    k_line_df = db_init(ss, sqlContext, is_debug)
    # 获取实时数据
    data = get_realtime_data(adjust_period)

    # 计算
    run(ss, data, k_line_df, today, is_debug, csv_path)
