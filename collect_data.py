import os
import pandas as pd


path = os.getcwd() + '/pk_data.csv'
path_list = os.listdir(path)
result_df = pd.DataFrame()
for the_path in path_list:
    tmp_path = os.path.join(path,the_path)
    try:
        tmp_df = pd.read_csv(tmp_path)
    except Exception as e:
        print('异常:'+str(e))
        print(tmp_path+'，路径不存在')
        continue
    if not tmp_df.empty:
        result_df = result_df.append(tmp_df)
col = ['sec_cd','var_cl','mkt_cl','pub_dt','pdi','mdi','k_value','d_value','kdj_s','pasr','f10','pi','new_pi','pk_sign']
result_df[col].to_csv('all_pk_data.csv', index=None)