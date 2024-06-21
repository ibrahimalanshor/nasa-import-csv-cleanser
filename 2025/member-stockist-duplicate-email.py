import csv
import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi

mongo_uri = 'mongodb://127.0.0.1:27017'
mongo_client = MongoClient('localhost', server_api=ServerApi('1'))
db = mongo_client.nasa_import

def get_csv_path(filename, dir='files'):
    return os.path.join(os.path.dirname(__file__), f'./{dir}/{filename}')

pipeline = [
    {
        "$lookup": {
            "from": "stockists",
            "localField": "email",
            "foreignField": "email",
            "as": "stockists"
        }
    },
    {
        "$match": {
            "stockists": {"$ne": []}
        }
    },
    {"$project": {"code": 1, "name": 1, "pin": 1, "_id": 0}}
]

result = list(db.members.aggregate(pipeline))
csv_file_path = get_csv_path("member-stockist-email-duplicate.csv", "result")

try:
    os.remove(csv_file_path)
except:
    pass

with open(csv_file_path, 'w', newline='') as csv_file:
    fieldnames = ['code', 'name', 'pin']
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    
    csv_writer.writeheader()
    csv_writer.writerows(result)