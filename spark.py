import streamlit as st
from pyspark import SparkConf, SparkContext
import collections
import os

header = st.container()
#Set master node on a local machine not in cluster
#setappname appears in spark UI
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

#CREATE sparkContext object
sc = SparkContext(conf = conf)

#load data into SC
lines = sc.textFile("/Users/angelokhan/Documents/Projects/Spark_project/u.data")

#extract data we care about
#spliting each into individual fields by white space
#the uses[2] to take out the rating value
ratings = lines.map(lambda x: x.split()[2])

#count unique values
result = ratings.countByValue()

#sorts results of dictionary
sortedResults = collections.OrderedDict(sorted(result.items()))

with header:
#prints results
    for key, value in sortedResults.items():
        st.text("{}, {}".format(key, value))
        print("%s %i" % (key, value))

sc.stop()
