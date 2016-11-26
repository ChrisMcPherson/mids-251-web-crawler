from pymongo import MongoClient

client = MongoClient()
db = client.test

cursor = db.restaurants.find({"_id": "randomURLhash1"})

for document in cursor:
    print(document)