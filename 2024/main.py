import os
import math
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
import progressbar

mongo_uri = "mongodb+srv://ibrahimalanshor:65cZx8wS7s0kvjrt@cluster0.bw1af.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
db = mongo_client.nasa_import

def import_stockist_penjualan():
    print('Importing raw stockist penjualan')

    filepath = os.path.join(os.path.dirname(__file__), './files/stockist-penjualan.csv')
    chunks = pd.read_csv(filepath, chunksize=1000, encoding='latin1', keep_default_na=False)
    filesize = len(pd.read_csv(filepath, encoding='latin1').index)

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
                        'email': row['EMAIL'] if row['EMAIL'] else f'{row["KDST"].lower()}@naturalnusantara.co.id'
                    }},
                    upsert=True
                )
                for _, row in df.iterrows()
            ]

            db.stockists.bulk_write(payload)

            bar.update(1000)

def import_stockist_keuangan():
    print('Importing raw stockist keuangan')

    filepath = os.path.join(os.path.dirname(__file__), './files/stockist-keuangan.csv')
    chunks = pd.read_csv(filepath, chunksize=1000, encoding='latin1', keep_default_na=False)
    filesize = len(pd.read_csv(filepath, encoding='latin1').index)

    with progressbar.ProgressBar(max_value=filesize) as bar:
        for df in chunks:
            payload = [
                UpdateOne(
                    {'code': row['KODEST']},
                    {'$set': {
                        'code': row['KODEST'],
                        'name': row['NMAST'],
                        'bank_name': row['KODEBANK'],
                        'bank_branch_name': row['CABANG'],
                        'bank_account_name': row['NMABANK'],
                        'bank_account_number': int(row['NOREK']) if type(row['NOREK']) is int else row['NOREK'],
                        'order': row['URUT'],
                    }},
                    upsert=True
                )
                for _, row in df.iterrows()
            ]

            db.stockists.bulk_write(payload)

            bar.update(1000)

def import_stockist_bonus():
    print('Importing raw stockist bonus')

    filepath = os.path.join(os.path.dirname(__file__), './files/stockist-bonus.csv')
    chunks = pd.read_csv(filepath, chunksize=1000, encoding='latin1', keep_default_na=False)
    filesize = len(pd.read_csv(filepath, encoding='latin1').index)

    with progressbar.ProgressBar(max_value=filesize) as bar:
        for df in chunks:
            payload = [
                UpdateOne(
                    {'code': row['KODEST']},
                    {'$set': {
                        'code': row['KODEST'],
                        'member_code': row['KDEDST'],
                        'name': row['NMAST'],
                        'city': row['KTAST'],
                        'phone': row['TLPST'],
                        'mobile_number': row['HP'],
                        'period': row['TGLMASUK'],
                        'pin': row['PIN'],
                        'email': row['EMAIL'],
                        'upline_code': row['KODEUPL'],
                        'upline_name': row['NMAUPL'],
                        'area': row['KAREA'],
                        'address': row['ALMST'],
                        'bank_account_number': row['NMRREK'],
                        'bank_name': row['NMABANK'],
                        'bank_branch_name': row['CBNBANK'],
                        'order': row['URUT'],
                    }},
                    upsert=True
                )
                for _, row in df.iterrows()
            ]

            db.stockists.bulk_write(payload)

            bar.update(1000)


import_stockist_penjualan()
import_stockist_keuangan()
import_stockist_bonus()

# mongoexport --collection=stockists --db=nasa_import --type=csv --out=2024/result/stockists.csv --fields=code,name,address,address2,phone,area,order,is_active,email,bank_name,bank_branch_name,bank_account_name,bank_account_number "mongodb+srv://ibrahimalanshor:65cZx8wS7s0kvjrt@cluster0.bw1af.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"