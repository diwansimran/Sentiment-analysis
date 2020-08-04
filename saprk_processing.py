#!/usr/bin/env python
# coding: utf-8

#starting spark
import os
os.environ["SPARK_HOME"]="/usr/local/Cellar/apache-spark/2.4.5/libexec/"
exec(open(os.path.join(os.environ["SPARK_HOME"], 'python/pyspark/shell.py')).read())

#taking input file in RDD
inp_file = sc.textFile("inputToSpark.txt")

#splitting lines by new line character
flatmap1=inp_file.flatMap(lambda line:line.split("\n"))

#splitting by space and generating words
flatmap2=flatmap1.flatMap(lambda line:line.split(" "))

#list of words which needs to be counted from txt file
keep=["education","canada","university","dalhousie","expensive","good schools","good school","bad schools","bad school","poor schools","poor school","faculty","computer science","graduate"]


broadcast = sc.broadcast(keep)

#keeping only those words in text which are required to be counted
flatmap3=flatmap2.filter(lambda x:x in broadcast.value)

#mapping key value pairs
map1=flatmap3.map(lambda flatmap3: (flatmap3, 1))

#reducing by key and taking sum of values
reduceresult=map1.reduceByKey(lambda a,b:a +b) 

#making RDD from list of words to be counted
map_list=sc.parallelize(keep)

#creating key value pairs from list
map_list1=map_list.map(lambda map_list: (map_list, 0))

#joining word count result and lists with summing values
join1=map_list1.union(reduceresult).reduceByKey(lambda x,y : x+y)

#writing in output file
join1.saveAsTextFile("sparkoutput")



