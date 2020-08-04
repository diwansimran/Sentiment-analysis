#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 18:56:06 2020

@author: simran
"""
import re
import pandas as pd
import json
import math
import ast

#reading input file
with open('input/news.json') as json_file:
    data = json.load(json_file)

#lists to store articles, description and title    
articles=[]
description=[]
title=[]

#function to clean data
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

#writing separate files for each article
count=1
for d in data:
    name=str(count)+".txt"
    if(d['content']!='none'):
        with open("articles/"+name,'a') as w_file:
            count=count+1
            #cleaning data and writing into file
            d['title']=clean_data(d['title'])
            d['content']=clean_data(d['content'])
            d['description']=clean_data(d['description'])
            w_file.write(str(d))

#storing number of articles            
no_of_articles=count

#search words
search_query=['canada','university','dalhousie university','halifax','business']
document_containing_term=[] #list to store total number of documents which contains specific term
N_by_df=[] #list to store N/df
log_n_by_df=[] #list to store Log10(N/df)

#calculating required fields
for word in search_query:
    word_article_count=0 #it will hold the count of articles in which search word is found
    count=1
    for i in range(1,no_of_articles):
        name=str(count)+".txt"
        f = open("articles/"+name, "r") #opening and reading file
        count=count+1
        article_current=f.read()
        #increasing counter if word is found in current article
        if(word in article_current):
            word_article_count+=1
    document_containing_term.append(word_article_count) #storing count of documents (df)
    ndf=(float(no_of_articles)/float(word_article_count)) #calculating N/df
    N_by_df.append(ndf)
    log_n_by_df.append(math.log10(ndf)) #calculating and storing log10(N/df)

#creating data frame to store findings    
df=pd.DataFrame(columns=['search query','df','N/df','log10N/df'])
df['search query']=search_query
df['df']=document_containing_term
df['N/df']=N_by_df
df['log10N/df']=log_n_by_df

#storing data frame into csv file
df.to_csv('TF-IDF.csv')

article_content=[] #articles with occurence of word canada
article_num=[] #article number
total_words=[] #total words in article
frequency=[] #frequency of canada in that article
relative_freq=[] #relative frequency of canada in that article

count=1
count2=1
#calculating required fields for articles that have word canada
for i in range(1,no_of_articles):
    name=str(count2)+".txt"
    f = open("articles/"+name, "r") #opening file
    count2=count2+1
    article_current=f.read() #reading current article
    res = ast.literal_eval(article_current) #converting current article into dictionary format
    #if article contains word canada then, calculate required fileds for that article
    if 'canada' in article_current:
        article_num.append(count) #storing article number
        article_content.append(res['content']) #storing article content
        count+=1 #increasing counter
        sp=article_current.split() #splitting current article by space
        total_words.append(len(sp)) #calculating total number of words
        freq=0 #initializing frequency of cananda as zero
        for s in sp:
            if 'canada' in s:
                freq+=1 #increasing frequency counter if canada is found
        frequency.append(freq) #storing frequency
        relative_freq.append(freq/len(sp)) #calculating and storing relative frequency

article_num_str=[]
for i in range(0,len(article_num)):
    article_num_str.append("Article #"+str(article_num[i]))

#creating dataframe  for storing findings
        
df1=pd.DataFrame(columns=['Canada appeared in '+str(len(article_content))+' documents','total_words(m)','frequency(f)'])
df1['Canada appeared in '+str(len(article_content))+' documents']=article_num_str
df1['total_words(m)']=total_words
df1['frequency(f)']=frequency

df1.to_csv('article_word_frequency_canada.csv') #writing output in csv file

#finding article with highest frequency of word canada
max=0
max_ind=0
for i in range(0,len(frequency)):
    if(frequency[i]>max):
        max=frequency[i]
        max_ind=i

#printing article content with maximum frequency of word canada
print(article_content[max_ind])

#finding article with highest relative frequency of word canada
max=0
max_ind=0
for i in range(0,len(relative_freq)):
    if(relative_freq[i]>max):
        max=relative_freq[i]
        max_ind=i

#printing article content with maximum relative frequency of word canada        
print(article_content[max_ind])

#writing output in file
file1 = open("highest_relative_freq_article.txt","a") 
file1.write(article_content[max_ind])
file1.close()

