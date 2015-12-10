from pymongo import MongoClient
from bson.code import Code
import pymongo
import datetime
import time

# mongoimport --host=127.0.0.1 -d microblog -c microblogData --type csv --file /Users/koskinap/Projects/DS_CW2/microblogDataset_COMP6235_CW2.csv --headerline

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata

#q5-q7
dbitems = col.find()
text_len = 0
hashtags_count = 0
for i in dbitems:
	tempText = i['text']
	if isinstance(tempText,basestring) == False:
		tempText = str(tempText)

	text_len += len(tempText)
	hashtags_count += tempText.count('#')

print(text_len)
print(hashtags_count)