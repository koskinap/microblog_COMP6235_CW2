from pymongo import MongoClient
from bson.code import Code
import pymongo

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata

#find total number of records:1.459.861
records_number=col.count()
#print(records_number)


#q1-119.231
unique_users = col.distinct('id_member')
print('Number of unique users is equal to ' + str(len(unique_users)))

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

print('Percentage of tweets made by 10 top users ' + str(100.0*tweet_sum/records_number) + '%')

client.close()