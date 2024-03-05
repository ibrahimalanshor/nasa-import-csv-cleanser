import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
import progressbar

mongo_uri = 'mongodb://127.0.0.1:27017'
mongo_client = MongoClient('localhost', server_api=ServerApi('1'))
db = mongo_client.nasa_import

def truncate_stockists():
    db.stockists.delete_many({})

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
                        'office_email': f'{row["KDST"]}@naturalnusantara.co.id',
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
                            'stockist_type': parse_stockist_type(row['KODEST']),
                            'email': f'{row["KODEST"]}@naturalnusantara.co.id',
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
            members = list(db.members.find({
                'code': {'$in': df['KDEDST'].tolist()}
            }))
            member_hash = {item['code']: {**item} for item in members if 'code' in item}

            payload_stockists = []
            payload_members = []

            for _, row in df.iterrows():
                is_has_member = row['KDEDST'] in member_hash
                
                payload_stockists.append(UpdateOne(
                    {'code': row['KODEST']},
                    {
                        '$set': {
                            'member_code': member_hash[row['KDEDST']]['code'] if is_has_member else None,
                            'member_name': member_hash[row['KDEDST']]['name'] if is_has_member else None,
                            'cellphone': row['HP'],
                            'period': row['TGLMASUK'],
                            'pin': row['PIN'],
                            'email': parse_email(row['EMAIL'], row['KODEST']),                        
                            'office_email': f'{row["KODEST"]}@naturalnusantara.co.id',
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
                        }
                    },
                    upsert=True
                ))

                if (is_has_member):
                    payload_members.append(UpdateOne(
                        {'code': member_hash[row['KDEDST']]['code']},
                        {
                            '$set': {
                                'stockist_code': row['KODEST'],
                                'stockist_name': row['NMAST'],
                            }
                        },
                        upsert=True
                    ))

            db.stockists.bulk_write(payload_stockists)
            db.members.bulk_write(payload_members)

            bar.update(1000)

def export_stockists():
    os.system(f'mongoexport --collection=stockists --db=nasa_import --type=csv --out=2024/result/stockists.csv --fields=stockist_type,member_code,member_name,code,name,address,address2,phone,area,order,is_active,email,office_email,bank_name,bank_branch_name,bank_account_name,bank_account_number,cellphone,kta,pulau,period,pin,upline_code,upline_name "{mongo_uri}"')

def replace_duplicate_values():
    pipeline = [
        {"$group": {"_id": { "$toLower": "$email" }, "duplicates": {"$push": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicate_emails = list(db.stockists.aggregate(pipeline))

    bulk_operations = []
    for duplicate in duplicate_emails:
        duplicates_to_null = duplicate["duplicates"][1:]
        bulk_operations.append(
            UpdateOne(
                {"_id": {"$in": duplicates_to_null}},
                {"$set": {"email": None}}
            )
        )

    if bulk_operations:
        result = db.stockists.bulk_write(bulk_operations)
        print(f"Duplicate Detected: {result.modified_count}")

def parse_email(email, code):
    if (type(email) != str or len(email) < 5):
        return f'{code.lower()}@naturalnusantara.co.id'
    else:
        return email

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

truncate_stockists()
import_stockist_penjualan()
import_stockist_keuangan()
import_stockist_bonus()
replace_duplicate_values()
export_stockists()