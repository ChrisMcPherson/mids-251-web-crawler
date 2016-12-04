from pymongo import MongoClient

# database: cdsearchdb      collection: webpages
client = MongoClient('mongodb://169.45.131.5/cdsearchdb')
db = client.cdsearchdb

all_docs_cursor = db.webpages.find()

for document in all_docs_cursor:
    print(document)
    break

