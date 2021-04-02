#!/usr/bin/env python
# coding: utf-8

# In[61]:


from os import environ
import pandas as pd
import json
import boto3


# In[44]:


aws_access_key_id = environ.get('aws_access_key_id')
aws_secret_access_key = environ.get('aws_secret_access_key')


# In[45]:


def read_json_s3(aws_credentials,bucket,filepath):
    session = boto3.Session(aws_credentials["key"],aws_credentials["secret"])
    s3 = session.resource('s3')
    content_object = s3.Object(bucket, filepath)
    file_content = content_object.get()['Body'].read()
    json_content = json.loads(file_content)
    return json_content


# In[57]:


def unnest_json_country(json_object):
    countries = []
    for country_code in json_object.keys():
        json_object[country_code]["country_code"] = country_code
        countries.append(json_object[country_code])
    return countries


# In[47]:


def df_write_s3_parquet_file(aws_credentials,df,bucket,filepath):
    df.to_parquet("s3://{bucket}/{filepath}".format(
        bucket=bucket,
        filepath=filepath),
                      storage_options=aws_credentials)
    return df


# In[52]:


def main():
    aws_credentials = { "key": aws_access_key_id, "secret": aws_secret_access_key}
    bronze_bucket = 'cars-benchmark-bronze'
    silver_bucket = 'cars-benchmark-silver'
    src_filepath = 'github/countries.json'
    trg_filepath = 'github/countries.parquet'
    json_countries = read_json_s3(aws_credentials,bronze_bucket,src_filepath)
    countries = unnest_json_country(json_countries)
    df = pd.DataFrame(countries)
    df_write_s3_parquet_file(aws_credentials,df,silver_bucket,trg_filepath)
    return 0


# In[60]:


if __name__ == "__main__":
    # execute only if run as a script
    main()

