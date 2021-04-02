#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import boto3
from os import environ


# In[2]:


aws_access_key_id = environ.get('aws_access_key_id')
aws_secret_access_key = environ.get('aws_secret_access_key')


# In[3]:


def download_github_masterdata_file(url):
    request = requests.get(url)
    return request.text


# In[4]:


def upload_file_to_s3(session,bucket,file):
    s3 = session.resource('s3')
    object = s3.Object(bucket, file["file_path"])
    object.put(Body=file["file_content"])


# In[7]:


def main():
    s3_session = boto3.Session(aws_access_key_id,aws_secret_access_key)
    bronze_bucket = 'cars-benchmark-bronze'
    github_master_file_url='https://raw.githubusercontent.com/annexare/Countries/master/data/languages.json'
    s3_filedir = 'github'
    s3_filename = 'languages.json'
    s3_filepath = s3_filedir + '/'  + s3_filename
    file = {"file_path" : s3_filepath, "file_content": download_github_masterdata_file(github_master_file_url)}
    upload_file_to_s3(s3_session,bronze_bucket,file)
    return 0


# In[8]:


if __name__ == "__main__":
    # execute only if run as a script
    main()

