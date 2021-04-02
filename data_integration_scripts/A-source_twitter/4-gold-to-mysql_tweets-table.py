#!/usr/bin/env python
# coding: utf-8

# In[48]:


import pandas as pd
import boto3
from sqlalchemy import create_engine
from os import environ


# In[44]:


aws_access_key_id = environ.get('aws_access_key_id')
aws_secret_access_key = environ.get('aws_secret_access_key')


# In[45]:


mysql_user = environ.get('mysql_user')
mysql_user_password = environ.get('mysql_user_password')
mysql_host = environ.get('mysql_host')
mysq_database = environ.get('mysq_database')


# In[42]:


def df_read_tweets_from_s3(aws_credentials,bucket,filedir):
    session = boto3.Session(aws_credentials["key"],aws_credentials["secret"])
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(bucket)
    df_all_tweets = pd.DataFrame()
    for my_bucket_object in my_bucket.objects.filter(Prefix=filedir + '/').all():
        df = pd.read_csv("s3://{bucket}/{filepath}".format(
                bucket=bucket,
                filepath=my_bucket_object.key),
                         encoding='utf-8',
                         storage_options=aws_credentials)
        df_all_tweets = pd.concat([df_all_tweets,df])
    return df_all_tweets


# In[55]:


def df_write_tweets_in_mysql(mysql_credentials,df):
    engine = create_engine("mysql://{user}:{password}@{host}/{database}?charset=utf8mb4".format(
        user=mysql_credentials["user"],
        password=mysql_credentials["user_password"],
        host=mysql_credentials["host"],
        database=mysql_credentials["database"])
                          )
    df.to_sql(con=engine, name='cars_all_brands_tweets', if_exists='replace')


# In[54]:


def main():
    aws_credentials = { "key": aws_access_key_id, "secret": aws_secret_access_key}
    mysql_credentials = {"user": mysql_user, "user_password":mysql_user_password, "host": mysql_host, "database": mysq_database}
    gold_bucket = 'cars-benchmark-gold'
    filedir = 'cars_all_brands_tweets'
    df = df_read_tweets_from_s3(aws_credentials,gold_bucket,filedir)
    df_write_tweets_in_mysql(mysql_credentials,df)
    return 0


# In[58]:


if __name__ == "__main__":
    # execute only if run as a script
    main()

