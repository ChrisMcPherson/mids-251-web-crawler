# pip install pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Connect and indicate success or failure
try:
  conn = MongoClient('mongodb://169.45.131.5:27017')
  # print "Success"
except ConnectionFailure, e:
  print "Failure: %s" % e

# print "Database Names"  
# print conn.database_names()
db = conn.cdsearchdb

doc_count =  db.webpages.count()
print doc_count # 534,762

# for i in range(0,10):
  # html = db.webpages.find()[i]['html']
  # raw_url = db.webpages.find()[i]['raw_url']

  # print "Size: ", len(html), raw_url
  

## Works
## Keys: _id, html, raw_url
# cursor = db.webpages.find()
# print type(cursor)
# print db.webpages.find()[50]
# print
# print db.webpages.find()[100:102]

## Works
## Keys: _id, html, raw_url
# cursor = db.webpages.find()
# for document in cursor:
  # for key in document:
    # print "key: %s, value: %s" % (key, document[key])
    # print document['raw_url']
    

## Works  
## [u'cdsearchdb', u'local', u'test', u'admin']
# print conn.database_names()

# db = conn.cdsearchdb
# cursor = db.users.find()
# print db.webpages.count() # 534,762

# for document in cursor:
  # print len(document)

  
## Works
# client = MongoClient('mongodb://169.45.131.5:27017')
# db = client.cdsearchdb
# cursor = db.webpages.find()
# for document in cursor:
    # print(document)
    # break
    
    