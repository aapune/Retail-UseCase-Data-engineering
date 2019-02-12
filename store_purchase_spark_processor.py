#!/usr/bin/env python
# coding: utf-8

# In[27]:


# ---- author - Aniruddha Anikhindi ------
# Program for data analysis for retail store and purchase log analysis
#Reason for using spark - possibility of million of records in logs 

import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as Fun
from pyspark.sql.functions import max


#sc - spark context - Enamble below 2 lines when loadig first time
#from pyspark import SparkContext
#sc = SparkContext()

sql_context = SQLContext(sc)

#Here we assume log files are ordered for Entry-Leave data
#If log file contains unordered data - then it should be grouped first with customeruuid,shopname and then by ascending order of timestamp

store_event_file_path = r'StoreEvent_2017-10-10.csv'
purchase_event_file_path = r'Purchases_2017-10-10.csv'
parquet_file_name = r'store_purchase_analysis_2017-10-10.parquet'

raw_rdd_se = sc.textFile(store_event_file_path)
headers_se = raw_rdd_se.first()
rdd_se_without_header = raw_rdd_se.filter(lambda x: x != headers_se)
rdd_se_splitted = rdd_se_without_header.map(lambda l: l.split(","))
rdd_store_events = rdd_se_splitted.map(lambda p: Row(date_se=p[0], custuuid=str(p[1]).strip(), eventname=str(p[2]).strip(), 
                                   shopname=str(p[3]).strip(), timestamp_se= str(p[4]).strip()))
df_se = sql_context.createDataFrame(rdd_store_events)
df_grouped_se = df_se.groupBy("custuuid","shopname").agg(Fun.max("date_se").alias('date_se'), 
                                                   Fun.min("timestamp_se").alias('from_ts'),
                                                   Fun.max("timestamp_se").alias('to_ts')).orderBy('custuuid', ascending=True)
df_grouped_se.registerTempTable('SE')


raw_rdd_pe = sc.textFile(purchase_event_file_path)
headers_pe = raw_rdd_pe.first()
rdd_pe_without_header = raw_rdd_pe.filter(lambda x: x != headers_pe)
rdd_pe_splitted = rdd_pe_without_header.map(lambda l: l.split(","))
rdd_purchase_events = rdd_pe_splitted.map(lambda p: Row(date_pe=p[0], custuuid=str(p[1]).strip(), amount=p[2], 
                                                   ts_pe=str(p[3]).strip(),ts_str=str(p[3]).strip()))
df_pe = sql_context.createDataFrame(rdd_purchase_events)
df_pe.registerTempTable('PE')

sql = r'SELECT se.date_se as DATE, se.custuuid as CUSTOMER_UUID, se.shopname as SHOP, IF(pe.amount is not null,pe.amount,0) as PURCHASE_AMOUNT, if(pe.ts_str is not null,pe.ts_str,"") as PURCHASE_TIME, se.from_ts as TIME_OF_ENTRY, se.to_ts as TIME_OF_OUT FROM SE se FULL OUTER JOIN PE pe ON  cast(se.from_ts as timestamp) <= cast(pe.ts_pe as timestamp) and cast(se.to_ts as timestamp) >= cast(pe.ts_pe as timestamp)'
df_sql = sql_context.sql(sql)
df_sql.show()

#df_final = df_sql.fillna( { 'PURCHASE_AMOUNT':0, 'PURCHASE_TIME':'' } ) --  One more option replace null with userdefined values

df_sql.write.mode('overwrite').parquet(parquet_file_name)
print('Parquet file generated with name ===> '+parquet_file_name)

df_gropued_shop = df_final.groupBy('SHOP').count().orderBy('count',ascending=False)
popular_shop = df_gropued_shop.toPandas().loc[0]['SHOP']
print('Most popular shop of the day ===> '+popular_shop)











# In[ ]:





# In[ ]:




