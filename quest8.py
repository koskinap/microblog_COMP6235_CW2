from pymongo import MongoClient
from bson.code import Code
import pymongo
import datetime
import time

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata

# Question 8
# Centroid coordinates 54.00366,-2.547855
# ukCLat = '54.00366'
# ukCLon = '-2.547855'

mapper = Code("""
		function() {
			var ukCLat = '54.00366';
			var ukCLng = '-2.547855';

			var cLng = this.geo_lng;
			var cLat = this.geo_lat;

			var loc = "";
			if (cLng < ukCLng && cLat >= ukCLat) {
					loc = "North-West";
			}else if(cLng < ukCLng && cLat < ukCLat){
				loc = "South-West";
			}else if (cLng >= ukCLng && cLat >= ukCLat) {
					loc = "North-East";
			}else if (cLng >= ukCLng && cLat < ukCLat){
					loc = "South-East";
			}
			emit(loc, 1);
		}
""")

reducer = Code("""
		function(key, value) {
			return Array.sum(value);
		}
""")

loc_counter = col.map_reduce(mapper,reducer,'locCounter')

topLoc = list(client.microblog.locCounter.find().sort('value', -1).limit(1))
print('The top location by messages is not suprisigly ' + topLoc[0]['_id'] + ' with ' + str(topLoc[0]['value']) + ' tweets.')


