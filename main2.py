from pymongo import MongoClient
from bson.code import Code
import pymongo
import datetime
import time

# mongoimport --host=127.0.0.1 -d microblog -c microblogData --type csv --file /Users/koskinap/Projects/DS_CW2/microblogDataset_COMP6235_CW2.csv --headerline

client = MongoClient('mongodb://localhost:27017/')

db = client.microblog
col = db.microdata

#find total number of records:1.459.861
time_sorted = col.find().sort('timestamp', pymongo.ASCENDING)
time_sorted2 = time_sorted.limit
time_delta = []
#temp = time_sorted.limit(3)
temp2 = time_sorted[0]['timestamp'].replace(' ','T')
prevDt = datetime.datetime.strptime(temp2, '%Y-%m-%dT%H:%M:%S')

for crawler in time_sorted:
	temp = crawler['timestamp'].replace(' ','T')
	currDt = datetime.datetime.strptime(temp, '%Y-%m-%dT%H:%M:%S')
	diff = (currDt-prevDt).total_seconds()
	print diff
	time_delta.append(diff)
	prevDt = currDt


client.close()