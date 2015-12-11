from pymongo import MongoClient
from bson.code import Code
import pymongo

# mongoimport --host=127.0.0.1 -d microblog -c microblogData --type csv --file /Users/koskinap/Projects/DS_CW2/microblogDataset_COMP6235_CW2.csv --headerline

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata

#find total number of records:1.459.861
records_number=col.count()

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


# average of 71 characters
print('Average tweet length is ' + str(text_len/records_number))
# 454972 hashtags
print('Total number of hashtags is ' + str(hashtags_count))