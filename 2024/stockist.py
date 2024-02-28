import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
import progressbar

mongo_uri = 'mongodb://127.0.0.1:27017'
mongo_client = MongoClient('localhost', server_api=ServerApi('1'))
db = mongo_client.nasa_import

def import_stockist_penjualan():
    print('Importing raw stockist penjualan')

    chunks = get_csv_chunk('stockist-penjualan.csv')
    filesize = get_csv_size('stockist-penjualan.csv')

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
                        'pulau': row['PULAU'],
                        'kta': row['KTAST'],
                        'is_active': 1 if row['PASIF'] != 'x' else 0,
                        'email': parse_email(row['EMAIL'], row['KDST']),
                        'stockist_type': parse_stockist_type(row['KDST'])
                    }},
                    upsert=True
                )
                for _, row in df.iterrows()
            ]

            db.stockists.bulk_write(payload)

            bar.update(1000)

def import_stockist_keuangan():
    print('Importing raw stockist keuangan')

    chunks = get_csv_chunk('stockist-keuangan.csv')
    filesize = get_csv_size('stockist-keuangan.csv')

    with progressbar.ProgressBar(max_value=filesize) as bar:
        for df in chunks:
            payload = [
                UpdateOne(
                    {'code': row['KODEST']},
                    {
                        '$set': {
                            'bank_name': row['KODEBANK'],
                            'bank_branch_name': row['CABANG'],
                            'bank_account_name': row['NMABANK'],
                            'bank_account_number': int(row['NOREK']) if type(row['NOREK']) is int else row['NOREK'],
                        },
                        '$setOnInsert': {
                            'code': row['KODEST'],
                            'name': row['NMAST'],
                            'order': row['URUT'],
                            'stockist_type': parse_stockist_type(row['KODEST'])
                        }
                    },
                    upsert=True
                )
                for _, row in df.iterrows()
            ]

            db.stockists.bulk_write(payload)

            bar.update(1000)

def import_stockist_bonus():
    print('Importing raw stockist bonus')

    chunks = get_csv_chunk('stockist-bonus.csv')
    filesize = get_csv_size('stockist-bonus.csv')

    with progressbar.ProgressBar(max_value=filesize) as bar:
        for df in chunks:
            payload = [
                UpdateOne(
                    {'code': row['KODEST']},
                    {
                        '$set': {
                            'member_code': row['KDEDST'],
                            'cellphone': row['HP'],
                            'period': row['TGLMASUK'],
                            'pin': row['PIN'],
                            'email': parse_email(row['EMAIL'], row['KODEST']),
                            'upline_code': row['KODEUPL'],
                            'upline_name': row['NMAUPL'],
                        },
                        '$setOnInsert': {
                            'code': row['KODEST'],
                            'name': row['NMAST'],
                            'phone': row['TLPST'],
                            'kta': row['KTAST'],
                            'area': row['KAREA'],
                            'address': row['ALMST'],
                            'bank_account_number': row['NMRREK'],
                            'bank_name': row['NMABANK'],
                            'bank_branch_name': row['CBNBANK'],
                            'order': row['URUT'],
                            'stockist_type': parse_stockist_type(row['KODEST'])
                        }
                    },
                    upsert=True
                )
                for _, row in df.iterrows()
            ]

            db.stockists.bulk_write(payload)

            bar.update(1000)

def export_stockists():
    os.system(f'mongoexport --collection=stockists --db=nasa_import --type=csv --out=2024/result/stockists.csv --fields=stockist_type,member_code,code,name,address,address2,phone,area,order,is_active,email,bank_name,bank_branch_name,bank_account_name,bank_account_number,cellphone,kta,pulau,period,pin,upline_code,upline_name "{mongo_uri}"')

def parse_email(email, code):
    return email if email else f'{code.lower()}@naturalnusantara.co.id'

def parse_stockist_type(code):
    if code.startswith('SCN'):
        return 'SCN'
    elif code.startswith('KYN'):
        return 'KYN'
    elif code.startswith('PRSH'):
        return 'PRSH'
    return 'STOCKIST'

def get_csv_chunk(filename):
    filepath = get_csv_path(filename)

    return pd.read_csv(filepath, chunksize=1000, encoding='latin1', keep_default_na=False)

def get_csv_size(filename):
    filepath = get_csv_path(filename)

    return len(pd.read_csv(filepath, encoding='latin1').index)

def get_csv_path(filename):
    return os.path.join(os.path.dirname(__file__), f'./files/{filename}')

import_stockist_penjualan()
import_stockist_keuangan()
import_stockist_bonus()
export_stockists()