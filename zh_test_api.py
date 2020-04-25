import requests
import pandas as pd
import time


# 初始化日志
import loginit
import logging
loginit.setup_logging('./logconfig.yml')


logging.info('begin!!!')
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
    while True:
        try:
            response = requests.get(url)
            break
        except Exception as e:
            logging.info('连接异常:'+str(e))
            time.sleep(3)
    null = ''
    response_dict = eval(response.text)
    data_dict = response_dict["ret"]
    ret_all_list = []
    for key, value in data_dict.items():
        ret_list = value['ret']
        if len(ret_list) == 0:
            print(key+'没有行情数据')
            continue
        ret_list = [[key]+ret[:-1] for ret in ret_list]
        ret_all_list.extend(ret_list)
    result_list.extend(ret_all_list)
    logging.info('500 stock ok')
tmp_stock_list = stock_list[i:]
url = "https://quote.investoday.net/quote/ex-klines?list="
url += ','.join(tmp_stock_list) + "&period=day&num=-250"
while True:
    try:
        response = requests.get(url)
        break
    except Exception as e:
        logging.info('连接异常:' + str(e))
        time.sleep(3)
response_dict = eval(response.text)
data_dict = response_dict["ret"]
ret_all_list = []
for key, value in data_dict.items():
    ret_list = value['ret']
    ret_list = [[key] + ret[:-1] for ret in ret_list]
    ret_all_list.extend(ret_list)
result_list.extend(ret_all_list)
logging.info('all stock ok')
col = ['sec_cd', 'timestamp', 'open', 'high', 'low', 'close', 'vol']
result_df = pd.DataFrame(result_list,columns=col)
print(result_df)
result_df.to_csv('realtime_data.csv',index=None)