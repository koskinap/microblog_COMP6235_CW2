from pymongo import MongoClient
from bson.code import Code
import pymongo
import datetime
import time

# mongoimport --host=127.0.0.1 -d microblog -c microblogData --type csv --file /Users/koskinap/Projects/DS_CW2/microblogDataset_COMP6235_CW2.csv --headerline

client = MongoClient('mongodb://localhost:27017/')
col = client.db.microdata

dates = datetime.datetime.strptime(col['timestamp'], '%Y-%m-%dT%H:%M:%S')
cur = dates.find({'timestamp': {'$gte': datetime.min, '$lte': datetime.max}})
print(gte)
# dates = list(col['timestamp'].find())
# print(dates)
#dates = datetime.datetime.strptime(col['timestamp'], '%Y-%m-%dT%H:%M:%S')
# print(dates.max)