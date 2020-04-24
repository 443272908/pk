"""
历史更新
"""
import pandas as pd
import time
from pyspark.sql import SQLContext, SparkSession, Window
import pyspark.sql.functions as func
import sys
from datetime import datetime
from pyspark.sql.types import FloatType, StringType, TimestampType, IntegerType, StructType, StructField, ArrayType
from new_data import fetch_com_dt_hist,check_trade_day
from new_measure import Measure
import numpy as np


# 初始化日志
from util import loginit
import logging
loginit.setup_logging('./logconfig.yml')

# 系统配置
#=================
# 读取配置文件
import os
import yaml
conf_path = './config.yml'
if not os.path.exists(conf_path):
    logging.exception('no found config yaml file')
    raise Exception('no found config yaml file')

conf = None
with open(conf_path) as f:
    conf = yaml.load(f, Loader=yaml.SafeLoader)

# 调试模式，设置true，会采样数据
is_debug = conf['sys']['is_debug']
# app_name = conf['sys']['app_name']
app_name = 'history_measure'
is_write_file = conf['sys']['is_write_file']
output_csv_path = conf['sys']['history_output_csv_path']
# output_csv_path = conf['sys']['daily_output_csv_path'] # 暂时当成日常路径来跑
data_source_csv_path = conf['sys']['history_data_source_csv_path']
# data_source_csv_path = conf['sys']['daily_data_source_csv_path'] # 暂时当成日常路径来跑
# =========================


def cal_performance(date, period, input_batch, output_batch):
    # 得到历史交易日
    hist_dt = fetch_com_dt_hist(date)
    pef_horizions = {
                    '1w': hist_dt.loc['B1W'].strftime('%Y%m%d'),
                     '1m': hist_dt.loc['B1M'].strftime('%Y%m%d'),
                     '3m': hist_dt.loc['B3M'].strftime('%Y%m%d'),
                     '6m': hist_dt.loc['B6M'].strftime('%Y%m%d'),
                     '1y': hist_dt.loc['B1Y'].strftime('%Y%m%d'),
                     '3y': hist_dt.loc['B3Y'].strftime('%Y%m%d'),
                     '5y': hist_dt.loc['B5Y'].strftime('%Y%m%d')

    }
    if period == 'all':
        start = None
    elif period == '1w':
        start = pef_horizions[period]
    elif period == '1m':
        start = pef_horizions[period]
    elif period == '3m':
        start = pef_horizions[period]
    elif period == '6m':
        start = pef_horizions[period]
    elif period == '1y':
        start = pef_horizions[period]
    elif period == '3y':
        start = pef_horizions[period]
    elif period == '5y':
        start = pef_horizions[period]
    ss = SparkSession \
        .builder \
        .appName(app_name + '_' + str(date) + '_' + period + '_' + str(is_debug)) \
        .getOrCreate()
    ss.sparkContext.setLogLevel('WARN')
    # 从csv读取数据 并进行格式转换
    schema = StructType([
        StructField('date', TimestampType(), True),
        StructField('sec_id', StringType(), True),
        StructField('nav', FloatType(), True),
        StructField('ret', FloatType(), True),
        StructField('stock', FloatType(), True),
        StructField('treasury', FloatType(), True),
        StructField('credit', FloatType(), True),
        StructField('bench_ret', FloatType(), True),
        StructField('fnd_category', IntegerType(), True),
    ])
    # ret_all_spark_df = ss.read.csv(data_source_csv_path + date + '/' + str(input_batch) + '/ret_all.csv', header=True,
    #                                schema=schema)
    ret_all_spark_df = ss.read.csv(data_source_csv_path + '20200320/1/ret_all.csv', header=True,
                                   schema=schema)
    # debug模式下只取部分基金
    if is_debug:
        logging.info('use debug')
        # sec_id_list = ['000006JK', '000028JK', '000134JK', '000135JK']
        # sec_id_list = ['005503JK', '005368JK', '004892JK', '150066JK',
        #         '000189JK', '000270JK', '000327JK']
        # sec_id_list = ['150066JK']
        # sec_id_list = ['006382JK']
        today_spark_df = ret_all_spark_df.filter(ret_all_spark_df.date == datetime.strptime(date, '%Y%m%d'))
        rank_w = Window.orderBy('sec_id')
        today_spark_df = today_spark_df.withColumn('row_no', func.row_number().over(rank_w))
        today_spark_df = today_spark_df.filter(today_spark_df.row_no <= 100).select('sec_id')
        ret_all_spark_df = ret_all_spark_df.join(today_spark_df, on='sec_id', how='inner')
        # ret_all_spark_df = ret_all_spark_df[ret_all_spark_df.sec_id.isin(sec_id_list)]
    else:
        logging.info('use release')
    # 只取date以前的数据  切片 date start是%Y%m%d 注意转换为timestamp
    ret_all_spark_df = ret_all_spark_df[ret_all_spark_df.date <= datetime.strptime(date, '%Y%m%d')]
    # date 小于最后一天
    w = Window.partitionBy('sec_id').orderBy('date').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ret_all_spark_df = ret_all_spark_df.withColumn('the_last_date', func.last('date').over(w))
    ret_all_spark_df = ret_all_spark_df.where(ret_all_spark_df.the_last_date >= datetime.strptime(date, '%Y%m%d'))
    if period == 'all':
        # 自定义函数
        udf_mean = func.udf(lambda x: float(pd.Series(x).mean()), FloatType())
        udf_std = func.udf(lambda x: float(pd.Series(x).std()), FloatType())
        udf_min = func.udf(lambda x: float(pd.Series(x).min()), FloatType())
        udf_max = func.udf(lambda x: float(pd.Series(x).max()), FloatType())
        udf_p25 = func.udf(lambda x: float(pd.Series(x).quantile(0.25)), FloatType())
        udf_median = func.udf(lambda x: float(pd.Series(x).median()), FloatType())
        udf_p75 = func.udf(lambda x: float(pd.Series(x).quantile(0.75)), FloatType())
        udf_skew = func.udf(lambda x: float(pd.Series(x).skew()), FloatType())
        udf_kurt = func.udf(lambda x: float(pd.Series(x).kurt()), FloatType())
        udf_start = func.udf(lambda x: str(x[0].strftime('%Y%m%d')), StringType())
        udf_end = func.udf(lambda x: str(x[-1].strftime('%Y%m%d')), StringType())
        udf_cagr = func.udf(lambda x: float(Measure.cal_cagr(pd.Series(x))), FloatType())
        udf_cumret = func.udf(lambda x: float(Measure.cal_cumret(pd.Series(x))), FloatType())
        udf_standard_deviation = func.udf(lambda x: float(Measure.cal_standard_deviation(pd.Series(x))), FloatType())
        udf_max_drawdown = func.udf(lambda x,y: float(Measure.cal_max_drawdown(pd.Series(x,index=y))), FloatType())
        udf_sharpe = func.udf(lambda x: float(Measure.cal_sharpe(pd.Series(x))), FloatType())
        udf_downside_deviation = func.udf(lambda x: float(Measure.cal_downside_deviation(pd.Series(x))), FloatType())
        udf_alpha = func.udf(lambda x,y,z,w,f: float(Measure.cal_alpha(pd.Series(x),
                                                                       pd.DataFrame({'stock': y,'treasury': z,'credit': w}),
                                                                       f)), FloatType())
        udf_marketbeta = func.udf(lambda x, y: float(Measure.cal_marketbeta(pd.Series(x), pd.Series(y))), FloatType())
        udf_information = func.udf(lambda x, y: float(Measure.cal_information(pd.Series(x), pd.Series(y))), FloatType())
        udf_treynor = func.udf(lambda x, y: float(Measure.cal_treynor(pd.Series(x), pd.Series(y))), FloatType())
        # 过滤基金数据长度不够
        ret_all_spark_df = ret_all_spark_df.withColumn('fund_length', func.count('date').over(w))
        ret_all_spark_df = ret_all_spark_df[ret_all_spark_df['fund_length'] >= 2]
        nt_val_spark_df = ret_all_spark_df[ret_all_spark_df.date == datetime.strptime(date, '%Y%m%d')].select('sec_id','nav').withColumnRenamed('nav', 'nt_val')
        # 做一下排序 保证ret按date有序  否則date在collect_list后不是順序
        ret_all_spark_df = ret_all_spark_df.withColumn('ret_list',func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('ret').over(w)))\
            .withColumn('stock_ret_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('stock').over(w)))\
            .withColumn('treasury_ret_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('treasury').over(w)))\
            .withColumn('credit_ret_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('credit').over(w)))\
            .withColumn('date_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(datetime.strptime('2020-03-06','%Y-%m-%d')))).otherwise(func.collect_list('date').over(w)))
        nav_agg_part_1 = ret_all_spark_df[ret_all_spark_df.date == ret_all_spark_df.the_last_date].select('sec_id',
                              'ret_list', 'stock_ret_list', 'treasury_ret_list', 'credit_ret_list', 'date_list', 'fnd_category')
        if is_debug:
            nav_agg_part_1.show()
        # 后面不需要用到ret_all_spark_df 把所有列全部drop掉
        ret_all_spark_df = ret_all_spark_df.drop('sec_id', 'date', 'nav', 'ret', 'stock', 'treasury', 'credit', 'bench_ret', 'fnd_category',
                                                 'the_last_date', 'fund_length',
                                                 'ret_list', 'stock_ret_list', 'treasury_ret_list', 'credit_ret_list', 'date_list'
                                                 )
        if is_debug:
            ret_all_spark_df.show()
        # 取当前日净值
        nav_agg_part_1 = nav_agg_part_1.join(nt_val_spark_df, on=['sec_id'], how='left')
        nav_agg_part_1 = nav_agg_part_1.withColumn('ret_mean', udf_mean('ret_list')) \
            .withColumn('ret_std', udf_std('ret_list')) \
            .withColumn('ret_min', udf_min('ret_list')) \
            .withColumn('ret_max', udf_max('ret_list')) \
            .withColumn('ret_p25', udf_p25('ret_list')) \
            .withColumn('ret_median', udf_median('ret_list')) \
            .withColumn('ret_p75', udf_p75('ret_list')) \
            .withColumn('ret_skew', udf_skew('ret_list')) \
            .withColumn('ret_kurtosis', udf_kurt('ret_list')) \
            .withColumn('ret_start', udf_start('date_list')) \
            .withColumn('cagr_sf', udf_cagr('ret_list'))\
            .withColumn('cumret_sf', udf_cumret('ret_list'))\
            .withColumn('vol_sf', udf_standard_deviation('ret_list'))\
            .withColumn('md_sf', udf_max_drawdown('ret_list','date_list'))\
            .withColumn('sharpe_sf', udf_sharpe('ret_list'))\
            .withColumn('dvol_sf', udf_downside_deviation('ret_list'))\
            .withColumn('alpha_sf', udf_alpha('ret_list','stock_ret_list','treasury_ret_list','credit_ret_list','fnd_category'))\
            .withColumn('beta_sf', udf_marketbeta('ret_list','stock_ret_list'))\
            .withColumn('ir_sf', udf_information('ret_list','stock_ret_list'))\
            .withColumn('treynor_sf', udf_treynor('ret_list','stock_ret_list'))
        # drop 掉中间列
        nav_agg_part_1 = nav_agg_part_1.drop('ret_list','stock_ret_list','treasury_ret_list','credit_ret_list' , 'date_list','fnd_category')
        if is_debug:
            nav_agg_part_1.show()
        if is_write_file:
            nav_agg_part_1.write.option('header', 'true').mode('overwrite').csv(output_csv_path + str(date) + "/"
                                                                                + str(output_batch) + "/" + period)
    else:
        # 自定義函數
        udf_cagr = func.udf(lambda x: float(Measure.cal_cagr(pd.Series(x))), FloatType())
        udf_cumret = func.udf(lambda x: float(Measure.cal_cumret(pd.Series(x))), FloatType())
        udf_aar = func.udf(lambda x: float(Measure.cal_aar(pd.Series(x))), FloatType())
        udf_alpha = func.udf(lambda x,y,z,w,f: float(Measure.cal_alpha(pd.Series(x),
                                                                       pd.DataFrame({'stock': y,'treasury': z,'credit': w}),
                                                                       f)), FloatType())
        udf_standard_deviation = func.udf(lambda x: float(Measure.cal_standard_deviation(pd.Series(x))), FloatType())
        udf_downside_deviation = func.udf(lambda x: float(Measure.cal_downside_deviation(pd.Series(x))), FloatType())
        udf_max_drawdown = func.udf(lambda x,y: float(Measure.cal_max_drawdown(pd.Series(x,index=y))), FloatType())
        udf_marketbeta = func.udf(lambda x, y: float(Measure.cal_marketbeta(pd.Series(x), pd.Series(y))), FloatType())
        udf_var = func.udf(lambda x: float(Measure.cal_var(pd.Series(x))), FloatType())
        udf_sharpe = func.udf(lambda x: float(Measure.cal_sharpe(pd.Series(x))), FloatType())
        udf_sortino = func.udf(lambda x: float(Measure.cal_sortino(pd.Series(x))), FloatType())
        udf_calmar = func.udf(lambda x: float(Measure.cal_calmar(pd.Series(x))), FloatType())
        udf_omega = func.udf(lambda x: float(Measure.cal_omega(pd.Series(x))), FloatType())
        udf_information = func.udf(lambda x, y: float(Measure.cal_information(pd.Series(x), pd.Series(y))), FloatType())
        udf_treynor = func.udf(lambda x, y: float(Measure.cal_treynor(pd.Series(x), pd.Series(y))), FloatType())
        udf_m_square = func.udf(lambda x, y: float(Measure.cal_m_square(pd.Series(x), pd.Series(y))), FloatType())
        udf_sterling = func.udf(lambda x: float(Measure.cal_sterling(pd.Series(x))), FloatType())
        udf_burke = func.udf(lambda x: float(Measure.cal_burke(pd.Series(x))), FloatType())
        udf_tail = func.udf(lambda x: float(Measure.cal_tail(pd.Series(x))), FloatType())
        udf_rachev = func.udf(lambda x: float(Measure.cal_rachev(pd.Series(x))), FloatType())
        udf_stability = func.udf(lambda x: float(Measure.cal_stability(pd.Series(x))), FloatType())
        udf_min_monthly_return = func.udf(lambda x, y: float(Measure.cal_min_monthly_return(pd.Series(x, index=y))), FloatType())
        udf_max_monthly_return = func.udf(lambda x, y: float(Measure.cal_max_monthly_return(pd.Series(x, index=y))),
                                          FloatType())
        udf_monthly_odds = func.udf(lambda x, y: float(Measure.cal_monthly_odds(pd.Series(x, index=y))),
                                          FloatType())
        udf_picking = func.udf(lambda x, y: float(Measure.cal_picking(pd.Series(x),pd.Series(y,name='stock'))), FloatType())
        udf_timing = func.udf(lambda x, y: float(Measure.cal_timing(pd.Series(x),pd.Series(y,name='stock'))), FloatType())
        udf_trackerror = func.udf(lambda x, y, z: float(Measure.cal_trackerror(pd.Series(x),pd.Series(y),z)), FloatType())
        # 过滤出开始日期在基金发行日期之后的数据
        ret_all_spark_df = ret_all_spark_df.withColumn('the_first_date', func.first('date').over(w))
        ret_all_spark_df = ret_all_spark_df[ret_all_spark_df.the_first_date <= datetime.strptime(start, '%Y%m%d')]
        # 取start日期之前的数据 切片
        ret_all_spark_df = ret_all_spark_df[ret_all_spark_df.date >= datetime.strptime(start, '%Y%m%d')]
        # 过滤基金数据长度不够 又进行了切片 所以需要重新统计基金长度
        ret_all_spark_df = ret_all_spark_df.withColumn('fund_length', func.count('date').over(w))
        ret_all_spark_df = ret_all_spark_df[ret_all_spark_df['fund_length'] >= 2]
        # 做一下排序 保证ret按date有序  否則date在collect_list后不是順序
        ret_all_spark_df = ret_all_spark_df.withColumn('ret_list',func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('ret').over(w)))\
            .withColumn('stock_ret_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('stock').over(w)))\
            .withColumn('treasury_ret_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('treasury').over(w)))\
            .withColumn('credit_ret_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('credit').over(w))) \
            .withColumn('bench_ret_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(0))).otherwise(func.collect_list('bench_ret').over(w))) \
            .withColumn('date_list', func.when(func.col('date') != func.col('the_last_date'), func.array(func.lit(datetime.strptime('2020-03-06','%Y-%m-%d')))).otherwise(func.collect_list('date').over(w)))
        nav_agg_part_2 = ret_all_spark_df[ret_all_spark_df.date == ret_all_spark_df.the_last_date].select('sec_id',
                              'ret_list', 'stock_ret_list', 'treasury_ret_list', 'credit_ret_list', 'bench_ret_list','date_list', 'fnd_category')
        if is_debug:
            nav_agg_part_2.show()
        # 后面不需要用到ret_all_spark_df 把所有列全部drop掉
        ret_all_spark_df = ret_all_spark_df.drop('sec_id', 'date', 'nav', 'ret', 'stock', 'treasury', 'credit', 'bench_ret', 'fnd_category',
                                                 'the_last_date', 'the_first_date', 'fund_length',
                                                 'ret_list', 'stock_ret_list', 'treasury_ret_list', 'credit_ret_list', 'bench_ret_list', 'date_list'
                                                 )
        if is_debug:
            ret_all_spark_df.show()
        nav_agg_part_2 = nav_agg_part_2.withColumn('cagr_' + period, udf_cagr('ret_list'))\
            .withColumn('cumret_' + period, udf_cumret('ret_list'))\
            .withColumn('aar_' + period, udf_aar('ret_list'))\
            .withColumn('alpha_' + period, udf_alpha('ret_list','stock_ret_list','treasury_ret_list','credit_ret_list', 'fnd_category'))\
            .withColumn('vol_' + period, udf_standard_deviation('ret_list'))\
            .withColumn('dvol_' + period, udf_downside_deviation('ret_list'))\
            .withColumn('md_' + period, udf_max_drawdown('ret_list', 'date_list'))\
            .withColumn('beta_' + period, udf_marketbeta('ret_list', 'stock_ret_list'))\
            .withColumn('var_' + period, udf_var('ret_list'))\
            .withColumn('sharpe_' + period, udf_sharpe('ret_list'))\
            .withColumn('sortino_' + period, udf_sortino('ret_list'))\
            .withColumn('calmar_' + period, udf_calmar('ret_list'))\
            .withColumn('omega_' + period, udf_omega('ret_list'))\
            .withColumn('ir_' + period, udf_information('ret_list','stock_ret_list'))\
            .withColumn('treynor_' + period, udf_treynor('ret_list','stock_ret_list'))\
            .withColumn('m_square_' + period, udf_m_square('ret_list','stock_ret_list'))\
            .withColumn('sterling_' + period, udf_sterling('ret_list'))\
            .withColumn('burke_' + period, udf_burke('ret_list'))\
            .withColumn('tail_' + period, udf_tail('ret_list'))\
            .withColumn('rachev_' + period, udf_rachev('ret_list'))\
            .withColumn('stability_' + period, udf_stability('ret_list'))
        if period in ['3m', '6m', '1y', '3y', '5y']:
            nav_agg_part_2 = nav_agg_part_2.withColumn('min_monthly_ret_' + period, udf_min_monthly_return('ret_list','date_list'))\
                .withColumn('max_monthly_ret_' + period, udf_max_monthly_return('ret_list','date_list'))\
                .withColumn('monthly_odds_' + period, udf_monthly_odds('ret_list', 'date_list'))
        if period in ['1m', '3m', '6m', '1y', '3y', '5y']:
            nav_agg_part_2 = nav_agg_part_2.withColumn('picking_' + period, udf_picking('ret_list', 'stock_ret_list'))\
                .withColumn('timing_' + period, udf_timing('ret_list', 'stock_ret_list'))\
                .withColumn('te_' + period, udf_trackerror('ret_list', 'bench_ret_list', 'fnd_category'))
        # drop 掉中间列表
        nav_agg_part_2 = nav_agg_part_2.drop('ret_list','stock_ret_list','treasury_ret_list','credit_ret_list','bench_ret_list','date_list','fnd_category')
        if is_debug:
            nav_agg_part_2.show()
        if is_write_file:
            nav_agg_part_2.write.option('header', 'true').mode('overwrite').csv(output_csv_path + str(date) + "/"
                                                                                + str(output_batch) + '/' + period)


def check_data_source_and_fetch_batch(date, period):
    """检查数据源文件和获取批次"""
    import pyhdfs
    hdfs_address = '192.168.10.218'
    hdfs_http_port = 50070
    hdfs_rpc_port = 8020
    client = pyhdfs.HdfsClient(hosts=["%s:%d" % (hdfs_address, hdfs_http_port)],
                               timeout=3,  # 超时时间，单位秒
                               max_tries=3,  # 节点重连秒数
                               retry_delay=5,  # 在尝试连接一个Namenode节点失败后，尝试连接下一个Namenode的时间间隔，默认5sec
                               user_name="hadoop",  # 连接的Hadoop平台的用户名
                               randomize_hosts=True,  # 随机选择host进行连接，默认为True
                               )
    # input_hdfs_remote_dir = data_source_csv_path + date + '/'
    input_hdfs_remote_dir = data_source_csv_path + '20200320/'
    output_hdfs_remote_dir = output_csv_path + date + '/'
    if not client.exists(input_hdfs_remote_dir):
        logging.info(input_hdfs_remote_dir + '路径不存在')
        is_success = False
        input_batch = None
    else:
        input_dirs = client.listdir(input_hdfs_remote_dir)
        input_batch = np.max([int(dir) for dir in input_dirs])
        if not client.exists(input_hdfs_remote_dir + str(input_batch) + '/' + '_SUCCESS'):
            logging.info(input_hdfs_remote_dir + str(input_batch) + '/_SUCCESS文件不存在')
            is_success = False
        else:
            is_success = True
            logging.info('数据源为第' + str(input_batch) + '批次文件')
    if not client.exists(output_hdfs_remote_dir):
        output_batch = 1
    else:
        output_dirs = client.listdir(output_hdfs_remote_dir)
        output_batch = np.max([int(dir) for dir in output_dirs])
        batch_dirs = client.listdir(output_hdfs_remote_dir + str(output_batch) + '/')
        # 8个进程都写完了 再开新的批次
        period_list = ['all','1w','1m','3m','6m','1y','3y','5y']
        if set(period_list).issubset(set(batch_dirs)):
            logging.info('第' + str(output_batch) + '批次所有进程全部跑完')
            output_batch = output_batch + 1
        else:
            logging.info('第'+str(output_batch)+'批次还有进程没有跑完')
            # 该批次的所有周期没有跑完 该周期的进程跑完后又起来了(防重跑防不住) 此时该周期不做
            if period in batch_dirs:
                is_success = False
    return is_success, str(input_batch), str(output_batch)


if __name__ == '__main__':
    now = time.strftime("%H:%M:%S")
    logging.info(str(now) + "begin!!!!")
    logging.info("sys.argv:" + str(sys.argv))
    if len(sys.argv) != 3:
        logging.info("please input two parameter:date and period,like 20200306 and 1w !!!")
        raise Exception('please input two parameter:date and period,like 20200306 and 1w !!!')
    date = sys.argv[1]
    try:
        logging.info("the date your input is:" + str(datetime.strptime(date, '%Y%m%d')))
    except Exception as e:
        logging.info(str(e))
        raise Exception('please input date like 20200306')
    if sys.argv[2] not in ['all','1w','1m','3m','6m','1y','3y','5y']:
        raise Exception('please input parameter: all,1w,1m,3m,6m,1y,3y,5y!!!')
    # 规范date的格式
    date = datetime.strptime(date, '%Y%m%d').strftime('%Y%m%d')
    period = sys.argv[2]
    RUNNING_FILE = '_RUNNING_HISTORY_'+date+'_' + period
    if os.path.exists(RUNNING_FILE):
        raise Exception("该进程正在运行")
    with open(RUNNING_FILE, 'w') as f:
        logging.info('写入防重跑文件')
    f.close()
    try:
        if check_trade_day(date):
            # 返回批次
            is_success, input_batch, output_batch = check_data_source_and_fetch_batch(date,period)
            logging.info('源数据为第'+str(input_batch)+'批次,输出路径为第'+str(output_batch)+"批次")
            if is_success:
                cal_performance(date, period, input_batch, output_batch)
            else:
                logging.info('没有数据源或该批次进程没有全部跑完')
        else:
            logging.info(date+' is not trade day!!!')
        now = time.strftime("%H:%M:%S")
        logging.info(str(now) + "end!!!!")
    except Exception as e:
        logging.info('出现异常'+str(e))
    os.remove(RUNNING_FILE)