from pymongo import MongoClient
from bson.code import Code
import pymongo

client = MongoClient('mongodb://localhost:27017/')
col = client.microblog.microdata

mapper = Code("""
		function() {
			var tempText = this.text;
			var splitText = tempText.toString().split(" ");

			for(i=0 ; i<splitText.length ;i++){
				punctText = splitText[i].trim();
				var punctRem = punctText.replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
				var finalStr = punctRem.replace(/\s{2,}/g," ");
				if (finalStr !== '')
					emit(finalStr,1);

			}

		}
""")

mapper2 = Code("""
		function() {
			var tempText = this.text;
			var splitText = tempText.toString().split(" ");

			for(i=0 ; i<splitText.length-1 ;i++){
				punctText = splitText[i].trim();
				punctText2 = splitText[i+1].trim();
				var punctRem = punctText.replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
				var punctRem2 = punctText2.replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
				var firstStr = punctRem.replace(/\s{2,}/g," ");
				var secStr = punctRem2.replace(/\s{2,}/g," ");
				
				finalStr = (firstStr + ' ' + secStr).trim();
				if (finalStr !== '')
					emit(finalStr,1);

			}

		}
""")

reducer = Code("""
		function(key, value) {
			return Array.sum(value);
		}
""")

unigram_counter = col.map_reduce(mapper,reducer,'unigramCounter')
unigram_list = list(client.microblog.unigramCounter.find().sort('value', -1).limit(10))

bigram_counter = col.map_reduce(mapper2,reducer,'bigramCounter')
bigram_list = list(client.microblog.bigramCounter.find().sort('value', -1).limit(10))


for uni in unigram_list:
	print ('Unigram' + uni['_id'] + 'has' + str(uni['value'])  +'appearances')

for bi in bigram_list:
	print ('Bigram' + bi['_id'] + 'has' + str(bi['value'])  +'appearances')
