import os.path
import csv
import re

fileDir = open('../microblogDataset_COMP6235_CW2.csv','rU')
exportFile = open('../cleanedData.csv', 'w+')

POS_INT = '^?[0-9]+$'
NEG_INT = '^-?[0-9]+$'


reader = csv.reader(fileDir)

head = False
for cdoc in reader:
	# Skip the first header line
	if head is True:

		if len(cdoc) < 6:
			continue

		#checkId = re.match(POS_INT,cdoc[0])
		#if not checkId is None
		checkIdMember = re.match(NEG_INT,cdoc[1])
		if not checkIdMember is None:
			cdoc[1] = str(abs(long(cdoc[1])))

		#if cdoc[3] 
		output = ','.join(cdoc) + '\n'
		exportFile.write(output);
	else:
		exportFile.write(','.join(cdoc) + '\n');	
		head = True

fileDir.close()
exportFile.close()



# mongoimport --host=127.0.0.1 -d microblog -c microblogData --type csv --file /Users/koskinap/Projects/DS_CW2/microblogDataset_COMP6235_CW2.csv --headerline
# mongoimport --host=127.0.0.1 -d microblog -c microdata --type csv --file /Users/koskinap/Projects/DS_CW2/cleanedData.csv --headerline
