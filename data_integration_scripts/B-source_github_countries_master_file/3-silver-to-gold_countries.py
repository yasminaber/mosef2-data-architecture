#!/usr/bin/env python
# coding: utf-8

# In[3]:


import boto3
import pandas as pd
from os import environ


# In[2]:


aws_access_key_id = environ.get('aws_access_key_id')
aws_secret_access_key = environ.get('aws_secret_access_key')


# In[8]:


def df_create_from_s3(aws_credentials,bucket,filepath):
    df = pd.read_parquet("s3://{bucket}/{filepath}".format(
        bucket=bucket,
        filepath=filepath),
                         storage_options=aws_credentials)
    return df


# In[11]:


def save_df_to_s3_csv(aws_credentials,bucket,df,filepath):
    df.to_csv("s3://{bucket}/{filepath}".format(
        bucket=bucket,
        filepath=filepath),
              storage_options=aws_credentials, index=False)


# In[14]:


def main():
    aws_credentials = { "key": aws_access_key_id, "secret": aws_secret_access_key}
    silver_bucket = 'cars-benchmark-silver'
    gold_bucket = 'cars-benchmark-gold'
    src_filepath = 'github/countries.parquet'
    trg_filepath = 'github/countries.csv'
    df = df_create_from_s3(aws_credentials,silver_bucket,src_filepath)
    save_df_to_s3_csv(aws_credentials,gold_bucket,df,trg_filepath)
    return 0


# In[20]:


if __name__ == "__main__":
    # execute only if run as a script
    main()

