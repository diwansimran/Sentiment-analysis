#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 20:04:18 2020

@author: simran
"""

import os
import tweepy as tw
import pandas as pd
import requests
import json
import re
import pymongo
from pymongo import MongoClient


#keys for accessing twitter search API
consumer_key= 'llcl3Ofr3rpp5RFnzBamuXJiQ'
consumer_secret= '1ypFgUbJji59C0DHlj0MxybUulw6gbE0DY0YctXRLbvaHVJV2O'
access_token= '1232331710412574720-ry12JVtnuPgzOcTAVHKB3RRwvyF4rI'
access_token_secret= 'tEPrtZSlP9I9WsKWz6WWe0ofqseFDaJYQgD3c4qP1Llun'

#authentication with twitter API
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)


#list of words to search in twitter API
search_words=["canada","university","dalhousie university","halifax","canada education"]

#list to store required list of tweets and data
users_locs=[]

#function for cleaning
def clean_data(inputStr):
    emoji_pattern = re.compile("["
         u"\U0001F600-\U0001F64F"
         u"\U0001F300-\U0001F5FF"
         u"\U0001F680-\U0001F6FF"
         u"\U0001F1E0-\U0001F1FF"
         u"\U00002702-\U000027B0"
         u"\U000024C2-\U0001F251"
         "]+", flags=re.UNICODE)
    inputStr=re.sub(r"[^a-zA-Z0-9]+", ' ', inputStr)  #removing special characters
    inputStr = emoji_pattern.sub(r'', inputStr) #removing emoji
    inputStr=re.sub(r"http\S+", "", inputStr)   #removing URLs
    inputStr=inputStr.lower() #converting string to lower case
    return inputStr

#extraction, cleaning and generation on JSON files of tweets
for s in search_words:
    #sending request
    tweets = tw.Cursor(api.search,
              q=s,
              lang="en", tweet_mode="extended").items(700)
    #getting full text in case from retweeted status, otherwise take full text field for tweet text
    for tweet in tweets:
        try:
            text=tweet._json.get("retweeted_status").get("full_text")
        except:
            text=tweet._json.get("full_text")
        #calling cleaniong function where required and storing in list    
        users_locs.append([clean_data(text), tweet.created_at, tweet.user.location, tweet.retweet_count, clean_data(tweet.user.name)])

#creating pandas dataframe  of tweets and giving column titles which will be used as keys in JSON files       
tweet_pd = pd.DataFrame(data=users_locs, 
                    columns=["text", "created_at","location","retweet_count","user_name"])
#storing data frame as JSON files
tweet_pd.to_json(r'tweet.json',orient='records',date_format='iso')




#processing news data

#key for news api
secret='e690dc4568f54782babd4bbf81b28f6f'
#list which will contain news data
news=[]
#words to search in news api
words = ['Canada', 'University', 'Dalhousie University', 'Halifax', 'Canada Education', 'Moncton', 'Toronto']

#data extraction from news api and also calling cleaning function
for word in words:
    #url for news api
    main_url = "http://newsapi.org/v2/everything?q="+word+"&pageSize=100&language=en&apiKey="+secret
    response = requests.get(main_url)
    articles=response.json().get("articles")
    #cleaning and getting required fields from response
    for article in articles:
        news.append([
            clean_data(str(article.get("author"))),
            clean_data(str(article.get("title"))), 
            clean_data(str(article.get("description"))), 
            clean_data(str(article.get("content")))])

#storing as a dataframe   
news_pd = pd.DataFrame(data=news, 
                    columns=["author", "title","description","content"])

#creating JSON file from dataframe for news data
news_pd.to_json(r'news.json',orient='records',date_format='iso') 


#data extraction and cleaning of movie data

#key to access OMDB api
secret='b5e18049'
#list to store data about movies
movies=[]
#search words for movie data
words = ['Canada', 'University', 'Moncton', 'Halifax', 'Toronto', 'Vancouver', 'Alberta', 'Niagara']

#data extractiona and cleaning
page=0
while len(movies)<500:
    page=page+1
    for word in words:  
        main_url ='http://www.omdbapi.com/?apikey='+secret+'&s='+word+'&page='+str(page)+'&type=movie'
        response = requests.get(main_url)
        search=response.json().get("Search")
        if search is not None:
            for s in search:
                movies.append([
                    clean_data(str(s.get("Title"))),
                    clean_data(str(s.get("Year"))),
                    str(s.get("imdbID")),
                    str(s.get("Type"))])

#storing as a dataframe    
movies_pd = pd.DataFrame(data=movies, 
                    columns=["title", "year","imdbid","type"])
                    
#list that will contain extended data on movies
movies_extended=[]

#fatching extended required movie data from imdb, cleaning it and and storing it in list
for i in movies_pd["imdbid"]:
    main_url ='http://www.omdbapi.com/?apikey='+secret+'&i='+i
    response = requests.get(main_url)
    s=response.json()
    movies_extended.append([
        clean_data(str(s.get("Title"))),
        clean_data(str(s.get("Year"))),
        s.get("Rated"),
        s.get("Released"),
        s.get("Runtime"),
        s.get("Genre"),
        s.get("Director"),
        s.get("Writer"),
        s.get("Actors"),
        s.get("Plot"),
        s.get("Language"),
        s.get("Country"),
        s.get("Ratings"),
        s.get("imdbRating"),
        s.get("imdbVotes"),
        s.get("imdbID"),
        s.get("Type")
        ])

#creating dataframe from list    
movies_extended_pd = pd.DataFrame(data=movies_extended, 
                    columns=["Title","Year","Rated","Released","Runtime","Genre","Director","Writer","Actors","Plot","Language","Country","Ratings","imdbRating","imdbVotes","imdbID","Type"])


#writing JSON file from movies data
movies_extended_pd.to_json(r'movies_extended.json',orient='records',date_format='iso')


#Creating connection with mongodb and storing data

#creating connection
client = MongoClient("mongodb://localhost:27017/")

#creating db
db=client["Assignment3_db"]

#creating 3 different collections to store movie, twitter and news data
coll=db["movie"]
coll1=db["twitter"]
coll2=db["news"]


#list which will be used to insert data into file
listToInsert=[]

#reading movies json file in list
listToInsert=[]
with open('movies_extended.json') as json_file:
        listToInsert = json.load(json_file)

#inseting list elements as documents in collection movie      
for doc in listToInsert:
    coll.insert_one(doc)
    
#reading tweet json file in list    
with open('tweet.json') as json_file:
        listToInsert = json.load(json_file)

#inseting list elements as documents in collection twitter        
for doc in listToInsert:
    coll1.insert_one(doc)

# reading news json file in list
with open('news.json') as json_file:
        listToInsert = json.load(json_file)

#inseting list elements as documents in collection news      
for doc in listToInsert:
    coll2.insert_one(doc)

#generating txt files for storing tweet texts and news description 
    
#this list will be used to store data on tweet text and news description
finalList=[]

#appending twitter text to list
for i in users_locs:
    finalList.append(i[0])


#appending news description to list
for i in news:
    finalList.append(i[2])

#generating input file for spark to count words
with open("inputToSpark.txt", "w") as text_file:
    for i in finalList:
        text_file.write(i+"\n")

#processing data of stream api

users_locs=[]

#StreamListener class
class StreamListener(tw.StreamListener):
    
    def on_status(self, status):
        #taking full text if extended tweet is available
        if hasattr(status,"extended_tweet"):
            text = status.extended_tweet["full_text"]
        else:
            text = status.text
        #appending data in list
        users_locs.append([clean_data(text), status.created_at, status.user.location, status.retweet_count, clean_data(status.user.name)])
        if len(users_locs)>500:
            return False
    def on_error(self, status_code):
        if status_code == 420:
            return False

stream_listener = StreamListener()
stream = tw.Stream(auth=api.auth, listener=stream_listener,tweet_mode="extended")
#filter
stream.filter(track=["canada","university","dalhousie university","halifax","canada education"])

#creating dataframe
tweet_pd = pd.DataFrame(data=users_locs, 
                    columns=["text", "created_at","location","retweet_count","user_name"])
#wtiting data in json format in file
tweet_pd.to_json(r'tweet_stream.json',orient='records',date_format='iso')

#reading json file
with open('tweet_stream.json') as json_file:
        listToInsert = json.load(json_file)
 
#inserting streaming api data into mongodb       
for doc in listToInsert:
    coll1.insert_one(doc)

#empty list
finalList=[]
#appending list with twitter text
for i in users_locs:
    finalList.append(i[0])

#generating input file for spark to count words 
with open("inputToSpark.txt", "a") as file_object:
    for i in finalList:
        file_object.write(i+"\n")