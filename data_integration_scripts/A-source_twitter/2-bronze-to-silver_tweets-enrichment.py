#!/usr/bin/env python
# coding: utf-8

# # Sommaire

# - A) Définition des identifiants
#     - A.1/ Initialisation clés Google Géo API
#     - A.2/ Initialisation clés AWS S3 API
# - B) Définition de la fonction 
# - C) Définition de la fonction

# In[153]:


import boto3
from os import environ
import pandas as pd
import googlemaps 
import json
import datetime


# **- A) Définition des identifiants**

# ***Initialisation de la clé pour la connexion à l'API Google Maps via un variable d'environnement déclarée sur le Cluster Databricks***

# In[154]:


gmaps_api_key = environ.get('gmaps_api_key')


# ***Initialisation des indetifiants de connexion de l'API AWS S3 via des variables d'environnement déclarées sur le Cluster Databricks***

# In[155]:


aws_access_key_id = environ.get('aws_access_key_id')
aws_secret_access_key = environ.get('aws_secret_access_key')


# Création d'une fonction qui accède au bucket à partir de la session s3, itère sur les répértoires et accède aux fichiers json dans chaque repertoire (marque) afin de pouvoir selectionner les champs qui nous interesse et les recupérer dans un seul et même dictionnaire

# In[156]:


def contact_brand_tweets_in_s3(session,bucket,brands,tweets):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(bucket)
    for brand in brands:
        for my_bucket_object in my_bucket.objects.filter(Prefix='brands' + '/' + brand).all():
            file_path = my_bucket_object.key
            content_object = s3.Object(bucket, file_path)
            file_content = content_object.get()['Body'].read().decode()
            json_content = json.loads(file_content)
            tweet = {}
            tweet["id"] = json_content["id"]
            tweet["text"] = json_content["text"]
            tweet["user_location"] = json_content["user"]["location"]
            tweet["lang"] = json_content["lang"]
            tweet["user_lang"] = json_content["user"]["lang"]
            tweet["place_type"] = json_content["place"]["place_type"] if json_content["place"] is not None else None
            tweet["place_name"] = json_content["place"]["name"] if json_content["place"] is not None else None
            tweet["place_full_name"] = json_content["place"]["full_name"] if json_content["place"] is not None else None
            tweet["place_country_code"] = json_content["place"]["country_code"] if json_content["place"] is not None else None
            tweet["place_country"] = json_content["place"]["country"] if json_content["place"] is not None else None
            tweet["brand"] = brand
            tweets.append(tweet)
    return tweets


# Création d'une fonction qui 

# In[157]:


def enrich_tweets_google_api_geocode_location(gmaps,tweets):
    api_call_count=0
    #loop for calling Google geocode API
    for tweet in tweets:
        if tweet["place_country_code"] is None:
            api_call_count=api_call_count+1
            tweet["user_location_google_geocode"] = gmaps.geocode(tweet["user_location"]) if len(tweet["user_location"]) != 0  else None
        else:
            tweet["user_location_google_geocode"] = None
    #loop for getting country code from google geocode results
    for tweet in tweets:
        tweet["user_location_google_geocode_country_code"] = None
        tweet["user_location_google_geocode_country_name"] = None
        if tweet["user_location_google_geocode"] is not None and len(tweet["user_location_google_geocode"]) != 0:
            json_dump = json.dumps(tweet["user_location_google_geocode"][0])
            json_content = json.loads(json_dump)
            json_dump = json.dumps(json_content["address_components"])
            json_content = json.loads(json_dump)
            for addr_compt in json_content:
                json_dump = json.dumps(addr_compt)
                addr_compt_json_content = json.loads(json_dump)
                if addr_compt_json_content["types"][0] == 'country':
                    tweet["user_location_google_geocode_country_code"]=addr_compt_json_content["short_name"]
                    tweet["user_location_google_geocode_country_name"]=addr_compt_json_content["long_name"]
        tweet.pop("user_location_google_geocode")
    return api_call_count


# In[158]:


def calculate_tweets_location(tweets):
    for tweet in tweets:
        if tweet["place_country_code"] is None:
            if tweet["user_location_google_geocode_country_code"] is not None:
                tweet["calculated_country_code"] = tweet["user_location_google_geocode_country_code"]
            elif tweet["lang"] is not None and tweet["lang"] != 'und':
                tweet["calculated_country_code"] = tweet["lang"].upper()
            elif tweet["user_lang"] is not None and tweet["user_lang"] != 'und':
                tweet["calculated_country_code"] = tweet["user_lang"].upper()
            else:
                tweet["calculated_country_code"] = 'Unspecified'
        else:
            tweet["calculated_country_code"] = tweet["place_country_code"]


# In[159]:


def save_tweets_to_parquet_in_s3(aws_credentials,bucket,brands,tweets):
    for brand in brands:
        df = pd.DataFrame(tweets)
        df.to_parquet(
            "s3://{bucket}/brands/{brand}/{file_name}.parquet".format(
                bucket=bucket,
                brand=brand,
                file_name=str(datetime.datetime.now()) + '-' +'twitter' + '-' + brand)
            , storage_options=aws_credentials)


# In[160]:


def move_tweets_to_processed_folder_in_s3(session,bucket,brands):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(bucket)
    for brand in brands:
        for my_bucket_object in my_bucket.objects.filter(Prefix='brands' + '/' + brand).all():
            s3.Object(bucket, 'processed' + '/' + my_bucket_object.key).copy_from(
                CopySource=bucket + '/' + my_bucket_object.key)
            my_bucket_object.delete()


# Définition de la fonction main avec la logique principale du programme 

# In[161]:


def main():
    gmaps = googlemaps.Client(gmaps_api_key)
    session = boto3.Session(aws_access_key_id,aws_secret_access_key)
    aws_credentials = { "key": aws_access_key_id, "secret": aws_secret_access_key}
    bronze_bucket = 'cars-benchmark-bronze'
    silver_bucket = 'cars-benchmark-silver'
    tweets=[]
    brands=[]
    brands.append('Mercedes')
    brands.append('Renault')
    brands.append('Peugeot')
    for brand in brands:
        brand = [brand]
        contact_brand_tweets_in_s3(session,bronze_bucket,brand,tweets)
        enrich_tweets_google_api_geocode_location(gmaps,tweets)
        calculate_tweets_location(tweets)
        save_tweets_to_parquet_in_s3(aws_credentials,silver_bucket,brand,tweets)
        move_tweets_to_processed_folder_in_s3(session,bronze_bucket,brand)
    return 0


# In[163]:


if __name__ == "__main__":
    # execute only if run as a script
    main()

