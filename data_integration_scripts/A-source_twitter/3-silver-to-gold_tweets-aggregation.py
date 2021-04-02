#!/usr/bin/env python
# coding: utf-8

# In[14]:


import boto3
import pandas as pd
from os import environ


# In[16]:


aws_access_key_id = environ.get('aws_access_key_id')
aws_secret_access_key = environ.get('aws_secret_access_key')


# In[23]:


def create_df_all_brands_tweets_from_s3(aws_credentials,bucket,brands):
    session = boto3.Session(aws_credentials["key"],aws_credentials["secret"])
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(bucket)
    all_brands_df = pd.DataFrame()
    for brand in brands:
        brand_df = pd.DataFrame()
        for my_bucket_object in my_bucket.objects.filter(Prefix='brands' + '/' + brand).all():
            df = pd.read_parquet("s3://{bucket}/{filepath}".format(
                bucket=bucket,
                filepath=my_bucket_object.key),
                                 storage_options=aws_credentials)
            brand_df = pd.concat([brand_df, df])
        all_brands_df = pd.concat([all_brands_df,brand_df])
    return all_brands_df


# In[39]:


def df_tweets_clean_remove_tweet_text(df):
    return df.drop(columns=["text"])


# In[37]:


def save_df_to_s3_csv(aws_credentials,bucket,df):
    filename = 'cars_all_brands_tweets.csv'
    filedir = 'cars_all_brands_tweets'
    df.to_csv("s3://{bucket}/{filepath}".format(
        bucket=bucket,
        filepath=filedir + '/' + filename),
              storage_options=aws_credentials, index=False)


# In[40]:


def main():
    aws_credentials = { "key": aws_access_key_id, "secret": aws_secret_access_key}
    silver_bucket = 'cars-benchmark-silver'
    gold_bucket = 'cars-benchmark-gold'
    brands=[]
    brands.append('Mercedes')
    brands.append('Renault')
    brands.append('Peugeot')
    df = create_df_all_brands_tweets_from_s3(aws_credentials,silver_bucket,brands)
    df = df_tweets_clean_remove_tweet_text(df)
    save_df_to_s3_csv(aws_credentials,gold_bucket,df)
    return 0


# In[45]:


if __name__ == "__main__":
    # execute only if run as a script
    main()

