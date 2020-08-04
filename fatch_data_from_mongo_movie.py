#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 18:34:04 2020

@author: simran
"""
import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

db=client["Assignment3_db"]

coll=db["movie"]

dic=[]

for doc in coll.find():
    dic.append(doc)

movie_result=[]
for d in dic:
    movie_result.append([d['Title'],d['Ratings'],d['Genre'],d['Plot']])

movie_result_pd=pd.DataFrame(data=movie_result, 
                    columns=["Title", "Ratings","Genre","Plot"])

movie_result_pd.to_json(r'movie_result.json',orient='records',date_format='iso')
