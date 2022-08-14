#!/usr/bin/env python
# coding: utf-8

# In[1]:

# Steps carried out in this code:-
# 1. Getting input from GCS
# 2. Processing the data to the requirements using sparksql in jupyter notebook
# 3. Saving the processed data back to GCS 



import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext


# In[2]:


from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession


# In[3]:


sc= SparkContext()
spark= SQLContext(sc)


# In[4]:


from datetime import date
current_date= date.today()
file_name= str(current_date)


# In[5]:

#------------------------------("gs://your-path-to-input-data-in-GCS-bucket"---)
#Here, the bucket name is :  "gs://tamilzh-etl/"

flights_data= spark.read.json("gs://tamilzh-etl/flights-data/"+file_name+".json")


# In[6]:


flights_data.registerTempTable("flights_data")


# In[7]:


qry1= """
    select
        flight_date,
        flight_num,
        round(avg(arrival_delay), 2) as avg_arrival_delay,
        round(avg(departure_delay), 2) as avg_departure_delay
    from 
        flights_data
    group by
        flight_num,
        flight_date
"""

avg_delays_by_flight_nums= spark.sql(qry1)
spark.sql(qry1).show()


# In[8]:


qry2= """
    select
        *,
        case 
            when distance between 0 and 500 then 1
            when distance between 501 and 1000 then 2
            when distance between 1001 and 2000 then 3
            when distance between 2001 and 3000 then 4
            when distance between 3001 and 4000 then 5
            when distance between 4001 and 5000 then 6
        END distance_category
    from 
        flights_data
"""

flights_data= spark.sql(qry2)
spark.sql(qry2).show()


# In[9]:


flights_data.registerTempTable("flights_data")


# In[10]:


qry3= """
    select
        flight_date,
        distance_category,
        round(avg(arrival_delay), 2) as avg_arrival_delay,
        round(avg(departure_delay),2) as avg_departure_delay
    from 
        flights_data
    group by 
        flight_date,
        distance_category
    order by 
        distance_category
"""

avg_delays_by_distance_category= spark.sql(qry3)
spark.sql(qry3).show()


# In[12]:


#Creating a new bucket for saving your processed data dynamically
#Here, the output bucket name is :  "de-course-project-outputs"

output_flight_nums= "gs://de-course-project-outputs/"+file_name+"/"+file_name+"_output_of_flight_nums"
output_distance_category= "gs://de-course-project-outputs/"+file_name+"/"+file_name+"_output_of_distance_category"

avg_delays_by_flight_nums.coalesce(1).write.format("json").save(output_flight_nums)
avg_delays_by_distance_category.coalesce(1).write.format("json").save(output_distance_category)


# In[ ]:




