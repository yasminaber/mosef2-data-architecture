#!/usr/bin/env python
# coding: utf-8

# # Sommaire

# - A) Definition des identifiants
#     - A.1/ Initialisation des identifiants de connexion de l'API Twitter
#     - A.2/ Initialisation des identifiants de connexion de l'API AWS S3
# - B) Définition d'une fonction permettant de récupérer des tweets
# - C) Définition d'une fonction permettant d'écrire les tweets dans un bucket S3 au format JSON
# - D) Définition de la fonction main avec la logique principale du programme

# In[1]:


import tweepy
import boto3
import json
from os import environ


# - **A) Definition des identifiants**

# ***Initialisation des identifiants de connexion de l'API Twitter via des variables d'environnement déclarées sur le Cluster Databricks***

# In[2]:


twitter_api_key = environ.get('twitter_api_key')
twitter_api_secret_key = environ.get('twitter_api_secret_key')
twitter_access_token = environ.get('twitter_access_token')
twitter_access_token_secret = environ.get('twitter_access_token_secret')


# ***Initialisation des identifiants de connexion de l'API AWS S3 via des variables d'environnement déclarées sur le Cluster Databricks***

# In[3]:


aws_access_key_id = environ.get('aws_access_key_id')
aws_secret_access_key = environ.get('aws_secret_access_key')


# - **B) Définition d'une fonction permettant de récupérer les tweets à travers des paramètres facile à variabliser**

# In[4]:


def fetch_tweets(auth, query,max_tweets):
    api = tweepy.API(auth)
    searched_tweets = []
    last_id = -1
    while len(searched_tweets) < max_tweets:
        count = max_tweets - len(searched_tweets)
        try:
            new_tweets = api.search(q=query, count=count, max_id=str(last_id - 1))
            if not new_tweets:
                break
            searched_tweets.extend(new_tweets)
            last_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # depending on TweepError.code, one may want to retry or wait
            # to keep things simple, we will give up on an error
            break
    return searched_tweets


# - ***C) Définition d'une fonction permettant d'écrire les tweets dans un bucket S3 au format JSON***

# In[5]:


def write_tweets_in_bucket(tweets,tweet_query,session,bucket):
    s3 = session.resource('s3')
    for tweet in tweets:
        object = s3.Object(bucket, 'brands' + '/' + tweet_query + '/' + 'created_at_' + str(tweet.created_at) + '-' + 'tweet_id_' + str(tweet.id) + '.json')
        object.put(Body=json.dumps(tweet._json).encode())


# ***- D) Définition de la fonction main avec la logique principale du programme***
# 
#    - Le programme principal crée les objects de sessions des API Twitter et S3
#    - Le programme principal appelle la fonction fetch_tweets pour récupérer les tweets de 3 marques (Renault, Peugeot et Mercedes)
#    - Le programme principal extrait les tweets au format JSON et les écrit sur le Bucket S3 représentant la couche Bronze du Data Llake
#    - Le programme principal n'effectue pas de transformation de données. La transformation/nettoyage des données sera fait du passage à la couche Silver du Data Lake dans un autre Programme Python

# In[11]:


def main():
    auth = tweepy.OAuthHandler(twitter_api_key, twitter_api_secret_key)
    auth.set_access_token(twitter_access_token, twitter_access_token_secret)
    s3_session = boto3.Session(aws_access_key_id,aws_secret_access_key)
    data_lake_bronze_layer_bucket = 'cars-benchmark-bronze'
    tweet_queries = []
    tweet_queries.append({"brand": "Renault","queries": ["Renault_fr", "Groupe_Renault"]})
    tweet_queries.append({"brand": "Peugeot","queries": ["PeugeotFR", "Peugeot"]})
    tweet_queries.append({"brand": "Mercedes","queries": ["MBFRANCE_", "MercedesBenz"]})
    max_tweets_per_query = 300
    for brand_tweet_queries in tweet_queries:
        for brand_tweet_query in brand_tweet_queries["queries"]:
            tweets = fetch_tweets(auth,brand_tweet_query,max_tweets_per_query)
            write_tweets_in_bucket(tweets,brand_tweet_queries["brand"],s3_session,data_lake_bronze_layer_bucket)
    return 0


# In[12]:


if __name__ == "__main__":
    # execute only if run as a script
    main()

