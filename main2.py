from pymongo import MongoClient
from bson.code import Code
import pymongo
import datetime
import time

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata


#find total number of records:1.459.861
records_number=col.count()

#
# dates = col.find().sort('timestamp', pymongo.ASCENDING)

pipeline2 = [
		{"$sort": { "timestamp": pymongo.ASCENDING} },
		{"$group": {"_id": "null", "maxdate": {"$last": "$timestamp"}, "mindate":{"$first":"$timestamp"}}}	
]

marginDates = list(col.aggregate(pipeline2, allowDiskUse=True))

maxd = marginDates[0]['maxdate'];
mind = marginDates[0]['mindate'];

print(maxd,mind)


# for d in dates:
# 	print(d['timestamp'])
exit()
#dates = datetime.datetime.strptime( , '%Y-%m-%dT%H:%M:%S')
cur = dates.find({'timestamp': {'$gte': datetime.min, '$lte': datetime.max}})
print(gte)

# time_delta = []
# temp2 = time_sorted[0]['timestamp'].replace(' ','T')
# prevDt = datetime.datetime.strptime(temp2, '%Y-%m-%dT%H:%M:%S')

# for crawler in time_sorted:
# 	temp = crawler['timestamp'].replace(' ','T')
# 	currDt = datetime.datetime.strptime(temp, '%Y-%m-%dT%H:%M:%S')
# 	diff = (currDt-prevDt).total_seconds()
# 	print diff
# 	time_delta.append(diff)
# 	prevDt = currDt


client.close()