from pymongo import MongoClient
#root:C3Y7lDPL@
client = MongoClient('mongodb://169.45.131.5/cdsearchdb')
#client = MongoClient()
db = client.cdsearchdb

#{"raw_url": "randomURLhash1"}
cursor = db.webpages.find()

for document in cursor:
    print(document)
    break
