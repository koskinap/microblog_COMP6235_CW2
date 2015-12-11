from pymongo import MongoClient
from bson.code import Code
import pymongo
import datetime
import time

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata


#find total number of records:1.459.861
records_number=col.count()

# Question 3 
pipeline2 = [
		{"$sort": { "timestamp": pymongo.ASCENDING} },
		{"$group": {"_id": "null", "maxdate": {"$last": "$timestamp"}, "mindate":{"$first":"$timestamp"}}}	
]

marginDates = list(col.aggregate(pipeline2, allowDiskUse=True))

maxd = marginDates[0]['maxdate'];
mind = marginDates[0]['mindate'];

print("Latest timestamp on " + maxd + " and earliest timestamp on" + mind)

# Transform into timestamp format and then in seconds to subtract them
# Question 4

dt_max = datetime.datetime.strptime(maxd, "%Y-%m-%d %H:%M:%S")
sec_max= time.mktime(dt_max.timetuple())
dt_min = datetime.datetime.strptime(mind, "%Y-%m-%d %H:%M:%S")
sec_min= time.mktime(dt_min.timetuple())

# Compute average time delta:0.471003383886
avgTimeDelta = (sec_max - sec_min)/(records_number-1)

print ("Average time delta = " + str(avgTimeDelta) + " seconds")

exit()

client.close()