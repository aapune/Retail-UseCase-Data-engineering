#!/usr/bin/env python
# coding: utf-8

# In[38]:


# This is a simple python file to

from IPython.display import display, HTML
import pandas as pd
#import pyarrow as pa
#import pyarrow.parquet as pq

store_event_file_path = r'StoreEvent_2017-10-10.csv'
purchase_event_file_path = r'Purchases_2017-10-10.csv'
store_event_parquet_file = 'store_event_frompython.parquet'
purchase_event_parquet_file = 'purchase_event_frompython.parquet'

df_se = pd.read_csv(store_event_file_path)
df_se_grp = df.groupby(['custuuid', 'shopname'], as_index=False).agg({"date": "min", "timestamp": ['min', 'max']})
df_se_grp.columns = ['CUSTUUID', 'SHOP', 'DATE', 'TIME_OF_ENTRY', 'TIME_OF_LEAVE']

display(df_se_grp)

#df_se_grp.to_parquet(store_event_parquet_file)
#df.to_parquet('myfile.parquet', engine='fastparquet')
#table = pa.Table.from_pandas(df_se_grp)
#pq.write_table(table, store_event_file_path)


#df2 = df.groupby(['custuuid', 'shopname']).size().reset_index(name='counts') - count column for enter/leave
#df3 = df.groupby(['custuuid', 'shopname'], as_index=False).agg(lambda x: sorted(set(x))) -- this will group with al values in col


df_pe = pd.read_csv(purchase_event_file_path)
display(df_pe)
#df_se_grp.to_parquet(purchase_event_parquet_file)









# In[20]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




