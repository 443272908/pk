from datetime import datetime, timedelta
from pyspark.sql import SparkSession, SQLContext, Window
import pyspark.sql.functions as func
import numpy as np
from pyspark.sql.types import FloatType


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


# ema按储存过程翻译过来 与传统存在差异 可查看compute_mkt_adx_v2
def get_ema(v, period):
    for i in range(len(v)):
        if i == 0:
            ema_value = v[i]
        else:
            ema_value = 1/period*v[i]+(period-1)/period*ema_value
    return float(ema_value)


def get_dmi_realtime_signal(spark_df, cal_trend_ratio):
    dmi_period = 30  # 周期并非传统的14
    # 对于最后一行 x y z为空值
    udf_max = func.udf(lambda x, y, z: float(max(max(abs(x), abs(y)), abs(z))
                                             if x is not None and y is not None
                                                and z is not None else np.nan), FloatType())
    udf_get_ema = func.udf(lambda x: float(get_ema(x, dmi_period)), FloatType())
    # di_plus di_minus tr 按储存过程翻译过来 与传统的算法存在差异 具体可查看compute_mkt_adx_v2
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
    w = Window.partitionBy('sec_cd')
    spark_df = spark_df.withColumn('sec_length', func.count('pub_dt').over(w))
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    spark_df = spark_df.withColumn('row_no', func.count('pub_dt').over(w))
    spark_df = spark_df.filter(spark_df.row_no >= spark_df.sec_length * cal_trend_ratio)
    spark_df = spark_df.select('sec_cd', 'var_cl', 'mkt_cl', 'pub_dt', 'pdi', 'mdi')
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


# 得到KDJ信号 具体代码参照matlab版本
def get_kdj_realtime_signal(spark_df, cal_trend_ratio):
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


if __name__ == "__main__":
    window_period = 250  # 最多拿250根bar
    min_cal_period = 60  # 最少需要60根bar
    cal_trend_ratio = 0.6  # 计算指标的数据、计算趋势的数据比例分配
    begin_date = (datetime.today() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")  # 最多追溯到三年前
    today = datetime.now().strftime("%Y-%m-%d")  # 今天
    today = '2020-03-20'
    is_debug = False  # 是否debug
    sql = """
    select sec_cd,var_cl,mkt_cl,pub_dt,high,low,close_p 'close' from
    (select sec_cd,var_cl,mkt_cl,pub_dt,cast(F0030 as float) 'high',cast(F0040 as float) 'low',cast(F0050 as float) 'close_p',
    ROW_NUMBER() over (partition by sec_cd,var_cl,mkt_cl order by pub_dt desc) as row_no 
    from cd_10_sec.dbo.QOT_D_BCK with(nolock) where var_cl = 'a' and is_vld = 1 and mkt_cl in ('s','z') and F0060 > 0) as t1
    where row_no <= %d and pub_dt >= '%s' and pub_dt <= '%s'
    """ % (window_period, begin_date, today)
    if is_debug:
        sql += " and sec_cd in ('000002','000903','300033','300627','600600') "
    spark = SparkSession \
        .builder \
        .appName("zh_cal_pk") \
        .getOrCreate()
    sqlContext = SQLContext(spark)
    k_line_df = dbquery(sqlContext, sql)
    w = Window.partitionBy('sec_cd')
    k_line_df = k_line_df.withColumn('sec_length', func.count('pub_dt').over(w))
    k_line_df = k_line_df.filter(k_line_df.sec_length >= min_cal_period)
    w = Window.partitionBy('sec_cd').orderBy('pub_dt')
    k_line_df = k_line_df.withColumn('current_length', func.count('pub_dt').over(w))
    psar_spark_df = get_psar_realtime_signal(k_line_df, cal_trend_ratio)
    dmi_spark_df = get_dmi_realtime_signal(k_line_df, cal_trend_ratio)
    psar_spark_df = psar_spark_df.join(dmi_spark_df, on=['sec_cd', 'var_cl', 'mkt_cl','pub_dt'], how='inner')
    @func.pandas_udf("sec_cd string,var_cl string,mkt_cl string," \
                     "pub_dt timestamp, pasr float,pi string, f10 float," \
                     "pdi float,mdi float,new_pi string", func.PandasUDFType.GROUPED_MAP)
    def amend_psar(group):
        group = group.sort_values('pub_dt')
        new_pi_list = group.pi.to_list()
        for i in range(1, len(group)):
            if new_pi_list[i-1] == 'Short':
                if group.iloc[i]['pi'] == 'Short':
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
                if group.iloc[i]['pi'] == 'Long':
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
                if group.iloc[i]['pdi'] - group.iloc[i]['mdi'] > 5 and group.iloc[i]['pi'] == 'Long':
                    new_pi_list[i] = 'Long'
                elif group.iloc[i]['mdi'] - group.iloc[i]['pdi'] > 5 and group.iloc[i]['pi'] == 'Short':
                    new_pi_list[i] = 'Short'
                else:
                    new_pi_list[i] = 'NoTrade'
        group['new_pi'] = new_pi_list
        return group
    psar_spark_df = psar_spark_df.groupby('sec_cd').apply(amend_psar)
    kdj_spark_df = get_kdj_realtime_signal(k_line_df, cal_trend_ratio)
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
    kdj_spark_df = kdj_spark_df.groupby('sec_cd').apply(get_kdj_signal)
    all_spark_df = psar_spark_df.join(kdj_spark_df, on=['sec_cd', 'var_cl', 'mkt_cl', 'pub_dt'], how='inner')
    all_spark_df = all_spark_df.withColumn('pk_sign', func.when((func.col('new_pi') == 'Long'), 1)\
        .when((((func.col('new_pi') == 'NoTrade') | (func.col('new_pi') == 'Short')) & (func.col('kdj_s') == 0)), 0)\
        .when((((func.col('new_pi') == 'NoTrade') | (func.col('new_pi') == 'Short')) & (func.col('kdj_s') == 1)), 1)\
        .otherwise(-1))
    # 对-1 用前一天的值填充
    def fill_forward(v):
        v = v[::-1]
        for i in range(len(v)):
            if v[i] != -1 or i == len(v)-1:
                return v[i]
    udf_fill_forward = func.udf(lambda x: float(fill_forward(x)), FloatType())
    all_spark_df = all_spark_df.withColumn('pk_sign', func.when(func.col('pk_sign') != -1, func.col('pk_sign')) \
                                           .otherwise(udf_fill_forward(func.collect_list('pk_sign').over(w))))
    all_spark_df.show(100)
    all_spark_df.write.option('header', 'true').mode('overwrite').csv("file:///tmp/a_zhangh/pk_signal.csv")




