#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

spark = SparkSession     .builder     .appName("xml parser")     .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "10g")     .config("spark.executor.instances","10")     .enableHiveSupport()    .getOrCreate()


# In[3]:


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks.spark.xml:spark-xml_2.11-0.9.0 pyspark-shell'


# In[4]:


df = spark.read.format("com.databricks.spark.xml").option("rowTag","Root").load("file:///home/cm2506/Documents/data/customer.xml")


# In[5]:


df.printSchema()


# In[5]:


df.count()


# In[7]:


field_info=[]
field_info.append(1)
def function_to_flatten_json(input_df):
    flat_columns = [c[0] for c in input_df.dtypes if c[1][:6] != 'struct' and c[1][:5] !="array"]
    print("flat_columns : ",flat_columns )
    nested_columns = [c[0] for c in input_df.dtypes if c[1][:6] == 'struct']
    array_columns=[c[0] for c in input_df.dtypes if c[1][:5] == 'array']
    print("array_columns",array_columns)
    print("nested_columns :",nested_columns)
    for ac in array_columns:
        input_df=input_df.select(flat_columns+nested_columns+[x for x in array_columns if x != ac ]+[F.explode(ac).alias(ac)])
        print(input_df.columns)
    input_df = input_df.select(flat_columns +array_columns+
                               [F.col(nested_column+'.'+c).alias(nested_column+'_'+c)
                                for nested_column in nested_columns
                                for c in input_df.select(nested_column+'.*').columns])
    field_info.pop()
    field_info.append(input_df)
    print("input_dataframe ",",".join([x[1][:6] for x in input_df.dtypes]))
    new_nested=[c[0] for c in input_df.dtypes if c[1][:6] == 'struct']
    new_array=[c[0] for c in input_df.dtypes if c[1][:5] == 'array']
    if new_nested or new_array :        
        function_to_flatten_json(input_df)
function_to_flatten_json(df)
field_info_df=field_info[0]


# In[8]:


field_info_df.printSchema()


# In[9]:


result=field_info_df.write.option("header","true").csv('customer')

