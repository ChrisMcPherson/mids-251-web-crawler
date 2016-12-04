from pymongo import MongoClient

client = MongoClient('mongodb://169.45.131.5/cdsearchdb')
db = client.cdsearchdb

all_docs_cursor = db.webpages.find()

for document in all_docs_cursor:
    print(document)
    break

# pip install pymongo

# database: cdsearchdb      collection: webpages

## Data Schema
# {
#     "_id": hashed_url,
#     "raw_url": raw_url,
#     "html": html
# }