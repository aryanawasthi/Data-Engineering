#!/usr/bin/env python
# coding: utf-8

# We have Covid data and we have to analyse the data to solve the required  questions.

# In[2]:


import pandas as pd
df=pd.read_csv("4. covid_19_data.csv")


# In[5]:


df.head()


# 1. Show the number of confirmed,Deaths and Recovered cases in each region.

# In[7]:


df.groupby("Region").sum(numeric_only=True)


# 2 Remove all the records when Confirmed cases is Less than 10.

# In[9]:


# to find the records where Confirmed Cases less than 10 we have to use the followin df.Confirmed<10

df[df.Confirmed<10]


# In[10]:


# to find values which are not in the same range we have
df[~(df.Confirmed<10)]


# 
# 3 In Which region maximum number of confirmed cases were recorded?

# In[15]:


# to find the region which have maximum cases we will group by region and the sort_values by the confirmed region
df.groupby("Region").Confirmed.sum().sort_values(ascending=False).head(10)


# 4. In which region minimum number of Deaths cases were recorded?
# 
# # group by region find death order it by lowest to highest 
# 

# In[18]:


# group by region and order by death cases and ascedning =False
df.groupby("Region").count().sort_values(by="Deaths",ascending=False)


# 5. How mant confirmed, Deaths & Recovered cases were reported from India till 29 April, 2020?
# find date before 29 April, 2020?

# In[23]:


# to find the deaths, recovered cases we will group by region and 
df[df.Region=="India"]


# 6.A Sort the entire data wrt to Confirmed Cases in ascending order?

# In[24]:


df.sort_values(by="Confirmed")


# 
# 6.B Sort the entired data with respecr to No Recovered Data in descending order.

# In[25]:


df.sort_values(by="Recovered",ascending=False)


# In[26]:


print("Aryan Awasthi")


# In[27]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Myfirst_application").master("local[*]").getOrCreate()


# In[29]:


data=spark.read.format("csv").option("header",True).option("inferSchema",True).option("path","/home/aryan/Jupyter/4. covid_19_data.csv").load()


# #1. Show the number of confirmed,Deaths and Recovered cases in each region.

# In[45]:


# to find the sum of all in each cases  we will group it by regions and then use the sum function to find the result
data.groupBy("Region").sum().collect()


# 2. Remove all the records when Confirmed cases is Less than 10.

# In[50]:


from pyspark.sql.functions import col
new_data=data.filter(col("Confirmed")>=10).collect()


# 3 In Which region maximum number of confirmed cases were recorded?

# In[56]:


# group by region and then order the values respectively.
data.groupBy("Region").sum().sort(col("sum(Confirmed)"),ascending=False).collect()


# 4. In which region minimum number of Deaths cases were recorded?`

# In[58]:


# group it by region and sort by the Deaths in increasing order.
data.groupBy("Region").sum().sort(col("sum(Deaths)"),ascending=True).take(10)


# 5. How mant confirmed, Deaths & Recovered cases were reported from India till 29 April, 2020?

# In[60]:


data.filter(col("Region")=="India").collect()


# In[ ]:


# As no multiple values are there for region India so we have our ansswer as like this.


# 6.A  Sort the entire data wrt to Confirmed Cases in ascending order?

# In[64]:


data.orderBy(col("Confirmed")).collect()


# 
