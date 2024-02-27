import os
import math
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
import progressbar

mongo_uri = "mongodb+srv://ibrahimalanshor:65cZx8wS7s0kvjrt@cluster0.bw1af.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
db = mongo_client.nasa_import

filepath = os.path.join(os.path.dirname(__file__), './files/stockist-penjualan.csv')
chunks = pd.read_csv(filepath, chunksize=1000)
filesize = len(pd.read_csv(filepath).index)

print('Importing raw stockist penjualan')
with progressbar.ProgressBar(max_value=filesize) as bar:
    for df in chunks:
        payload = [
            UpdateOne(
                {'code': row['KDST']},
                {'$set': {
                    'code': row['KDST'],
                    'name': row['NMAST'],
                    'address': row['ALMST'],
                    'address2': row['ALMST1'],
                    'phone': row['TLPST'],
                    'area': row['AREA'],
                    'order': row['URUT'],
                    'is_active': 1 if row['PASIF'] == 'x' else 0,
                    'email': row['EMAIL'] if type(row['EMAIL']) is str else f'{row["KDST"].lower()}@naturalnusantara.co.id'
                }},
                upsert=True
            )
            for _, row in df.iterrows()
        ]

        db.stockists.bulk_write(payload)

        bar.update(1000)