import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.types import StringType


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
for i in range(500, len(stock_list), 500):
    tmp_stock_list = stock_list[i-500:i]
    url = "https://quote.investoday.net/quote/ex-klines?list="
    url += ','.join(tmp_stock_list) + "&period=day&num=-250"
    response = requests.get(url)
    null = ''
    response_dict = eval(response.text)
    data_dict = response_dict["ret"]
    ret_all_list = []
    for key, value in data_dict.items():
        ret_list = value['ret']
        if len(ret_list) == 0:
            print(key+'没有行情数据')
            continue
        ret_list = [[key]+ret for ret in ret_list]
        ret_all_list.extend(ret_list)
    result_list.extend(ret_all_list)
tmp_stock_list = stock_list[i:]
url = "https://quote.investoday.net/quote/ex-klines?list="
url += ','.join(tmp_stock_list) + "&period=day&num=-250"
response = requests.get(url)
response_dict = eval(response.text)
data_dict = response_dict["ret"]
ret_all_list = []
for key, value in data_dict.items():
    ret_list = value['ret']
    ret_list = [[key] + ret for ret in ret_list]
    ret_all_list.extend(ret_list)
result_list.extend(ret_all_list)
col = ['sec_cd', 'timestamp', 'open', 'high', 'low', 'close', 'vol', 'money']
spark = SparkSession \
    .builder \
    .appName("zh_cal_pk") \
    .getOrCreate()
result_df = spark.createDataFrame(result_list, col)
result_df.withColumn('date', datetime.strptime(func.substring(func.col('timestamp').cast(StringType()),1 , 10)))
result_df.show()
# result_df = pd.DataFrame(result_list, columns=col)
# result_df['date'] = result_df['timestamp'].apply(lambda x: datetime.fromtimestamp(int(str(x)[:10])))
# def get_count(xdf):
#     xdf['count'] = len(xdf)
#     return xdf
# result_df = result_df.groupby('sec_cd').apply(lambda x: get_count(x))
# result_df.to_csv('')
# print(result_df)