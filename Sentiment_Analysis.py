"""
Created on Mon Apr  6 00:12:04 2020

@author: simran
"""
#Sentiment analysis
import re
import pandas as pd
import json

with open('input/tweet.json') as json_file:
    data = json.load(json_file)

#list to store tweet text
tweet_text=[]
for d in data:
    tweet_text.append(d['text'])

#list to store cleaned text of tweet    
tweet_text_clean=[]
#cleaning tweets
for tweet in tweet_text:
    tweet=re.sub("[\w ]","",tweet)
    tweet_text_clean.append(tweet)

#creating bag of words
BoW=[]
for tweet in tweet_text:
    BoW1={}
    tweet=tweet.split()
    for word in tweet:
        if word not in BoW1.keys():
            BoW1[word]=1  #inserting key with value 1
        else:
            BoW1[word]+=1 #increasing value related to key by 1
    BoW.append(BoW1) #appending bags to list of bag of words

polarity=[] #list to store polarity
match=[] #list to store all matched words
positive_match=[] #list to store words with positive match
negative_match=[] #list to store words with negative match

#finding polarity of each tweet 
for i in range(0,len(BoW)):
    bag=BoW[i]
    words=bag.keys()
    polarity_val=0
    m=[]
    positive=[] #temporary list to store positively matched words
    negative=[] #temporary list to store negatively matched words
    #matching each word of text with words in txt files of negative words and positive words
    for word in words:
        if(word in open('input/positive_words.txt').read().split()):
            polarity_val=polarity_val+1 #increasing value of polarity on positive match
            m.append(word)
            positive.append(word) #appending word to the list of words that match positively
        if(word in open('input/negative_words.txt').read().split()):
            polarity_val=polarity_val-1 #decreasing value of polarity on negative match
            m.append(word)
            negative.append(word) #appending word to the list of words that match negatively
    #classifying tweet as positive negative or neutral according to polarity value and storing polarity type in list
    if polarity_val>0:
        polarity.append("positive")
    elif polarity_val<0:
        polarity.append("negative")
    else:
        polarity.append("neutral")
    match.append(m)
    positive_match.append(positive) #storing list of positively matched words into positive_match list
    negative_match.append(negative)  #storing list of negatively matched words into negative_match list

#creating dataframe of tweet and sematic findings about tweet    
df=pd.DataFrame(columns=['tweet','match','positive_match','negative_match','polarity'])
df['tweet']=tweet_text
df['polarity']=polarity
df['match']=match
df['positive_match']=positive_match
df['negative_match']=negative_match

#storing all positively matched words into one list
all_positive=[]
for pos in positive_match:
    for p in pos:
        all_positive.append(p)
        
#storing all negatively matched words into one list
all_negative=[]
for neg in negative_match:
    for n in neg:
        all_negative.append(n)
        
df.to_csv('Sentiment_analysis_tweeter_output.csv') #writing sentiment analysis findings into csv file
positive_df=pd.DataFrame(columns=['Positive_words']) #dataframe of all positively matched words
negative_df=pd.DataFrame(columns=['Negative_words']) #dataframe of all negatively matched words

positive_df['Positive_words']=all_positive
negative_df['Negative_words']=all_negative

positive_df.to_csv('Positive_words_tweet.csv') #writing all positively matched words in a csv file
negative_df.to_csv('Negative_words_tweet.csv') #writing all negatively matched words in a csv file