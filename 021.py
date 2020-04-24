from pyspark.sql.types import StringType,FloatType,ArrayType,IntegerType,StructType,StructField,BooleanType,TimestampType
import pyspark.sql.functions as func
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Window
from datetime import datetime
import numpy as np
# from web_management.web.zh_temp.measure_for_021 import Measure
# from measure_for_021 import Measure
import pandas as pd
import numpy as np

# data = [
#     (1, 'a', 10),
#     (1, 'a', 9),
#     (2, 'a', 11),
#     (2, 'a', 13),
#     (1, 'b', 10),
#     (1, 'b', 6),
#     (2, 'b', 3),
#     (2, 'b', 13)
# ]

spark = SparkSession \
    .builder \
    .appName("macd") \
    .master("local[*]") \
    .getOrCreate()
data = [
    (1, 'assdaffd', 10),
    (3, 'fasfsaddfsa', 9),
    (2, 'aafdsdfsdsf', 11),
    (4, 'aafsdfdfsa', 13),]
# data = [
#     (1, 'a', 10),
#     (3, 'a', 9),
#     (2, 'a', 11),
#     (4, 'a', 13),
#     (5, 'a', 14),
#     (1, 'b', 10),
#     (2, 'b', 6),
#     (3, 'b', 3),
#     (4, 'b', 13),
#     (5, 'b', 14),
#     (6, 'b', 15),
#     (4, 'c', 11),
#     (3, 'c', 12),
#     (2, 'c', 9),
#     (5, 'c', None),
#     (6, 'c', None),
#     (7, 'c', 9),
#     (8, 'c', None),
#     (9, 'c', None),
#     (10, 'c', 9),
#     (5, 'd', None),
#     (6, 'd', None),
# (7, 'd', 1)
# ]
# data = [
# #     (1, 'a', 10.0, 11),
# #     (3, 'a', 9.0, 8),
# #     (2, 'a', 11.0, 12),
# #     (4, 'a', 13.0, 11),
# #     (2, 'b', 6.0, 7),
# #     (3, 'b', 3.0, 1),
# #     (4, 'c', 11.0, 13),
# #     (3, 'c', 12.0, 11),
# #     (2, 'c', np.nan, 8)
# # ]
# col = ["id", "class", "value1", "value2"]
col = ["id", "class", "value"]
df = spark.createDataFrame(data, col)
df = df.withColumn('sub_str',func.substring('class',1,3))
df.show()
# w = Window.orderBy('id')
# df.withColumn('row_no',func.row_number().over(w)).show()
# def get_mean(v):
#     return np.mean(v)
# udf_get_mean = func.udf(lambda x:get_mean(x),FloatType())
# w = Window.partitionBy('id')
# df.withColumn('the_mean',udf_get_mean(func.collect('vlaue').over(w)))
# df.show()
# # del df
# w = Window.partitionBy("class")
# df = df.withColumn('sec_length', func.count('id').over(w))
# w = Window.partitionBy("class").orderBy("id")
# df = df.withColumn('row_no', func.count('id').over(w))
# df = df.filter(df.row_no >= df.sec_length*0.5)
# df = df.withColumn('value_list',func.collect_list('value').over(w))
# df = df.withColumn('value3', func.lead('value1',1).over(w))
# udf_plus_one = func.udf(lambda x:float(x+1 if x is not None else np.nan),FloatType())
# df = df.withColumn("value3",udf_plus_one(func.lead('value1',1).over(w)-func.col('value1')))
# df = df.withColumn("value3",max(func.col("value1"), func.col("value2")))
df.show()
# udf_myfunc = func.udf(lambda x,y:float(pd.Series(x).mean()+y),FloatType())
# w = Window.partitionBy('class').orderBy('id').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
# df = df.withColumn('the_last_id', func.last('id').over(w))
# df = df.withColumn('the_value', func.lit([1,2,3].cast(ArrayType(IntegerType()))))
# udf_list = func.udf(lambda x: list([0]), ArrayType(IntegerType()))
# df = df.withColumn('the_value', udf_list('id'))
# df = df.withColumn('the_value', func.array(func.lit(datetime.strptime('2020-'))))
# df = df.withColumn('the_value', func.lit(np.array([1,2,3])))
# df = df.withColumn('the_value', func.col('the_value').cast(ArrayType(IntegerType())))
# df = df.withColumn('the_value', func.when(func.col('id') == func.col('the_last_id'),list([0])).otherwise(func.collect_list('value').over(w)))
# df = df.withColumn('the_value',udf_myfunc(func.collect_list('value').over(w), func.col('id')))
# df.show()
# df.concat_ws()
# w = Window.partitionBy('class').orderBy('id').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
# a = func.collect_list(df.value).over(w)
# df[a].show()
# a.show()
# print(a)
# df.show()
# df = df.drop('id','class','value')
# df.printSchema()

# df = df.na.fill({'value':float('nan')})
# df = df.replace(float('nan'),None)
# df = df.groupby('class').agg(func.collect_list('value').alias('value_list'))
# df.show()
# data = [
#     ('a',[1, 2, 3], [3, 2, 1], ['2019-01-03','2019-01-02','2019-01-01']),
# ('b',[1, 2, 3], [3, 2, 1], ['2019-01-03','2019-01-02','2019-01-01'])
# ]
# col = ['sec_id','a_list','b_list','d_list']
# df = spark.createDataFrame(data, col)
# df = df.withColumn('d_list',func.col('d_list').cast(ArrayType(TimestampType())))
# df.show()
# df.printSchema()
# udf_sort = func.udf(lambda x,y:list(pd.Series(x,index=y).sort_index()), ArrayType(IntegerType()))
# df = df.withColumn('a_list', udf_sort('a_list','d_list'))\
#     .withColumn('b_list', udf_sort('b_list','d_list'))\
#     .withColumn('d_list',func.sort_array('d_list'))
# df.show()
# df.show()
# df.show()
# df.withColumn('c',func.concat_ws('id'))
# df.select('id','class').distinct().show()
# w =Window.partitionBy('class')
# df = df.withColumn('the_max_value',func.max('value').over(w))\
#     .withColumn('the_min_value',func.min('value').over(w))
# df.show()
# df.show()
# w = Window.partitionBy('class')
# df = df.groupby('class').agg(func.collect_list('value').alias('value_list'))
# w = Window.partitionBy('class').orderBy('id').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
# df = df.withColumn('the_last_value', func.last('value').over(w))\
#     .withColumn('the_first_value',func.first('value').over(w))\
#     .withColumn('the_first_id',func.first('id').over(w))
# df = df.where((df.the_last_value.isNotNull())&(df.id < 8))
# df.show()
# df = df.withColumn('not_null',func.)
# df.withColumn('length',func.count('value').over(w)).show(100)
# df.withColumn('length',func.count('value').over(w))[]
# df.agg(*[(1-(func.count(c) /func.count('*'))).alias(c+'_missing') for c in df.columns]).show()
# df = df.agg[]
# udf_not_null_num =
# df.where(df.value.isNotNull()).withColumn('not_null_length', func.count('id').over(w)).show()
# print(id_list)
# df.where(df.value.isNotNull()).show()

# df = df.withColumn('non_null_length', func.count('value'))
# df.show()
# df.groupby('class').count().show()
# df.show()
# df.groupby('class').agg(func.count('value')).show()
# w = Window.partitionBy('class')
# a = df.where(df.value.isNotNull()).withColumn('not_null_length', func.count('id').over(w))
# df.show()
# a = df.groupby('class').agg('class_list', func.collect_list('class')).collect()
# df.filter(df.value.isNull()).show()
# df.select(func.isnull('value')).show()
# df.filter(func.isnull('value')).show()
# w = Window.partitionBy('class').orderBy('id')
# df = df.withColumn('value', func.last('value',ignorenulls=True).over(w))
# df = df.fillna(0)
# df.show()
# df = df.sort('class','id')
# aaa = df.groupby('class').min('id')
# aaa.show()
# w = Window.partitionBy('class')
# # df = df.withColumn('the_first_id',func.min('id').over(w))
# df = df[func.min('id').over(w) == 1]
# df.show()
# gdf = df.groupby('class')
# df_agg_1 = gdf.agg(func.collect_list('value').alias('value_list'))
# df_agg_2 = gdf.agg(func.collect_list('id').alias('id_list'))
# df_agg_2.show()
# df_agg = df_agg_1.join(df_agg_2,on=['class'],how='inner')
# udf_mean_old = func.udf(lambda x: float(np.mean(x)),FloatType())
# df_agg = df_agg.withColumn('mean_old',udf_mean_old('value_list'))
# udf_mean = func.udf(lambda x: float(Measure.get_mean(pd.Series(x), y=4)), FloatType())
# df_agg = df_agg.withColumn('mean', udf_mean('value_list'))
# udf_plus = func.udf(lambda x ,y: float(Measure.get_column_plus(pd.DataFrame({'a': x,'b': y}))), FloatType())
# df_agg = df_agg.withColumn('column_plus_column',udf_plus('value_list', 'id_list'))
# udf_index_one = func.udf(lambda x,y: float(Measure.get_index_one(pd.Series(x, index=y))), FloatType())
# df_agg = df_agg.withColumn('index_one',udf_index_one('value_list', 'id_list'))
# df_agg.show()
# filter_id = 5
# udf_filter = func.udf(lambda x: filter_id in x, BooleanType())
# # df_agg = df_agg.withColumn('flag', udf_filter('id_list'))
# df_agg = df_agg[udf_filter('id_list')]
# df_agg.show()
# filter_id = 1

# period = 3
# w =Window.partitionBy('class').orderBy('id').rowsBetween(1-period, Window.currentRow)
# df = df.withColumn('mean',func.avg('value').over(w))
# df.show()
# gdf = df.select('class').dropDuplicates()
# gdf.show()
# # 聚合 每个基金收拢成一条记录
# df_agg_1 = gdf.agg(func.collect_list('value').alias('value_list'))
# df_agg_2 =  gdf.agg(func.collect_list('id').alias('id_list'))
# df_agg = df_agg_1.join(df_agg_2,on=['class'],how='inner')
# def get_mean(v,x,flag):
#     if x == 'a':
#         # return np.nan
#         res = np.nan
#     else:
#         res = np.mean(v)+flag
#     return float(res)
# # udf_mean = func.udf(lambda x,y:get_mean(x,y,flag=3),FloatType())
# udf_mean = func.udf(get_mean,FloatType())
# aaa = 3
# df_agg = df_agg.withColumn('value_mean', udf_mean('value_list','class',aaa))
# df_agg.show()


# df.withColumn('mean')
# 自定义函数 mean
# udf_mean = func.udf(lambda x: float(pd.Series(x).mean()), FloatType())
# udf_max = func.udf(lambda x: float(pd.Series(x).max()), FloatType())
# udf_min = func.udf(lambda x: float(pd.Series(x).min()), FloatType())
# df_agg = df_agg.withColumn('mean', udf_mean('value_list'))\
#     .withColumn('max', udf_max('value_list'))\
#     .withColumn('min', udf_min('value_list'))
# df_agg.show()
# udf_cumprod = func.udf(lambda x: float(pd.Series(x).cumprod().iloc[-1]), FloatType())
# df = df.withColumn('value_list', udf_cumprod(func.collect_list(func.col('value')+1).over(w)))
# df = df.withColumn('cumprod',udf_cumprod('value_list'))
# df.show()
# df = df.withColumn('ret',func.col('value') - func.lag('value', 1).over(w))
# df = df.withColumn('ret_2',func.col('ret') - func.lag('ret', 1).over(w))
# df = df.replace('a',None).dropna()
# w = Window.partitionBy('class').orderBy('id')
# df = df.withColumn('ma3', func.when(func.col('id')).otherwise(func.avg('value').over(w)))
# df = df[df['class'].isin(['b','c'])]
# df = df[(df['id']+df['value'])>10]
# df = df.filter(func.abs(func.col('id'))+func.abs(func.col('id')) > 10)
# df.show()
# w = Window.partitionBy('class').orderBy('id').rowsBetween(Window.currentRow+1, Window.unboundedFollowing)
# w = Window.partitionBy('class').orderBy('id').rowsBetween(Window.unboundedPreceding, Window.currentRow)
# w = Window.partitionBy('class').orderBy('id').rowsBetween(Window.currentRow+1, Window.unboundedFollowing)
# w = Window.orderBy('id').partitionBy('class').rowsBetween(-2, Window.currentRow)
# df = df.withColumn('length', func.count('id').over(w))
# df = df.withColumn('value_list_in_after', func.collect_list('value').over(w))\
#     .withColumn('id_list_in_after', func.collect_list('id').over(w))
# df.show()
# def first_bigger_id(input1, input2, value):
#     for i in range(len(input1)):
#         if input1[i] >= value:
#             return input2[i]
# myUdf = func.udf(first_bigger_id, IntegerType())
# df = df.withColumn('first_bigger_id', myUdf('value_list_in_after','id_list_in_after','value'))
# df.show()


# def myFunc(input,flag):
#     sum = 0
#     for i in input:
#         sum += i
#     return sum/len(input)+flag
# myUdf = func.udf(lambda x: myFunc(x,3), FloatType())
# df = df.sort("class", "id")
# df = df.groupBy("class").agg(myUdf(func.collect_list("value")).alias("mean"))
# df.show()



# #
# @func.pandas_udf("A: str,B: str,C: int", func.PandasUDFType.GROUPED_MAP)
# def normalize(pdf):
#     C = pdf.C
#     return pdf.assign(C = C-C.mean())
# df = df.groupBy('A').apply(normalize)
# df.show()


# df = df.withColumn('data', func.concat_ws(',',func.col('B'),func.col('C')))
# df = df.groupBy('A').agg(func.collect_list('data').alias('data_list'))
# df = df.groupBy('A').agg(func.collect_list('data'))
# df.show()