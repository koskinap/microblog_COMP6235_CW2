from pymongo import MongoClient
from bson.code import Code
import pymongo

# mongoimport --host=127.0.0.1 -d microblog -c microblogData --type csv --file /Users/koskinap/Projects/DS_CW2/microblogDataset_COMP6235_CW2.csv --headerline

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata

#find total number of records:1.459.861
records_number=col.count()
print(records_number)
# n2 = col.distinct('id')
# print(len(rn2))

#q1-119.231
unique_users = col.distinct('id_member')
print(len(unique_users))

#q2-2.21%
pipeline = [
		{"$group": {"_id": "$id_member", "count": {"$sum": 1}}},
		{"$sort": {"count":-1}},
		{"$limit": 10}		
]

top10_users = list(col.aggregate(pipeline))
tweet_sum = 0

for c in top10_users:
	tweet_sum = tweet_sum + c['count']

print(100.0*tweet_sum/records_number)

client.close()


# for doc in q1:
# 	print(doc)