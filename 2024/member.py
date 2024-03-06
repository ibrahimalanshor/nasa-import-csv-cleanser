import os
import csv
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi

mongo_uri = 'mongodb://127.0.0.1:27017'
mongo_client = MongoClient('localhost', server_api=ServerApi('1'))
db = mongo_client.nasa_import

def truncate_members():
    db.members.delete_many({})

def import_member_master():
    print('Importing raw member master')

    cols = ['NMRDST','KDEDST','NMADST','KTPDST','JNSKLM','TGLLHR','MRTSTT','ALM1','ALM2','KTA','KDEPOS','TLP','HP','NMAPSN','LHRPSN','NMAPWR','LHRPWR','TMPBNS','NRKBANK','NMABANK','CBGBANK','NMRUPL','NMAUPL','PRD','NPWP','ANAK','EMAIL']

    chunks = get_csv_chunk('member-master.csv', cols)
    filesize = get_csv_size('member-master.csv', cols)

    inserted = 0

    for df in chunks:
        payload = [
            UpdateOne(
                {'code': row['KDEDST']},
                {'$set': {
                    'code': row['KDEDST'],
                    'temp_code': row['NMRDST'],
                    'name': row['NMADST'],
                    'ktp': parse_ktp(row['KTPDST']),
                    'sex': 'male' if row['JNSKLM'] == 'L' else 'female',
                    'birthdate': row['TGLLHR'],
                    'marital_status': 'single' if row['MRTSTT'] == 'N' else 'single',
                    'address': row['ALM1'],
                    'address2': row['ALM2'],
                    'kta': row['KTA'],
                    'postal_code': row['KDEPOS'],
                    'phone': row['TLP'],
                    'cellphone': row['HP'],
                    'spouse_name': row['NMAPSN'],
                    'spouse_birthdate': row['LHRPSN'],
                    'devisor_name': row['NMAPWR'],
                    'devisor_birthdate': row['LHRPWR'],
                    'bonus_office': row['TMPBNS'],
                    'bank_name': row['NMABANK'],
                    'bank_branch_name': row['CBGBANK'],
                    'upline_code': row['NMRUPL'],
                    'upline_name': row['NMAUPL'],
                    'register_code': parse_register_name(row['NMRDST']),
                    'register_name': parse_register_name(row['NMRDST']),
                    'period': row['PRD'],
                    'npwp_number': row['NPWP'],
                    'dependents_number': row['ANAK'],
                    'bank_account_number': int(row['NRKBANK']) if type(row['NRKBANK']) is int else row['NRKBANK'],
                    'email': parse_email(row['EMAIL'], row['KDEDST']),
                    'office_email': f'{row["KDEDST"]}@naturalnusantara.co.id',
                }},
                upsert=True
            )
            for _, row in df.iterrows()
        ]

        db.members.bulk_write(payload)

        inserted += 1000
        print(f'insert {inserted}/{filesize}')

def import_member_bonus():
    print('Importing raw member bonus')

    cols = ['NMRDST','KDEDST','NMADST','TGLLHR','NMRUPL','NMAUPL', 'PIN']

    chunks = get_csv_chunk('member-bonus.csv', cols)
    filesize = get_csv_size('member-bonus.csv', cols)

    inserted = 0

    for df in chunks:
        payload = [
            UpdateOne(
                {'code': row['KDEDST']},
                {
                    '$set': {
                        'pin': row['PIN']
                    },
                    '$setOnInsert': {
                        'code': row['KDEDST'],
                        'temp_code': row['NMRDST'],
                        'name': row['NMADST'],
                        'upline_code': row['NMRUPL'],
                        'upline_name': row['NMAUPL'],
                        'birthdate': row['TGLLHR'],
                        'email': parse_email('', row['KDEDST']),
                        'office_email': f'{row["KDEDST"]}@naturalnusantara.co.id',
                        'register_code': parse_register_name(row['NMRDST']),
                        'register_name': parse_register_name(row['NMRDST']),
                    }
                },
                upsert=True
            )
            for _, row in df.iterrows()
        ]

        db.members.bulk_write(payload)

        inserted += 1000
        print(f'insert {inserted}/{filesize}')

def export_member():
    os.system(f'mongoexport --collection=members --db=nasa_import --type=csv --out=2024/result/members.csv --fields=code,temp_code,name,ktp,sex,birthdate,marital_status,address,address2,kta,postal_code,phone,cellphone,spouse_name,spouse_birthdate,devisor_name,devisor_birthdate,bonus_office,bank_name,bank_branch_name,upline_code,upline_name,register_code,register_name,period,npwp_number,dependents_number,bank_account_number,email,office_email,pin "{mongo_uri}"')

def replace_duplicate_email():
    pipeline = [
        {"$group": {"_id": {"$toLower": "$email"}, "duplicates": {"$push": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicate_emails = list(db.members.aggregate(pipeline))

    bulk_operations = []
    for duplicate in duplicate_emails:
        duplicates_to_null = duplicate["duplicates"]
        bulk_operations.extend([
            UpdateOne(
                {"_id": duplicate_id},
                {"$set": {"email": None}}
            ) for duplicate_id in duplicates_to_null[1:]
        ])

    if bulk_operations:
        result = db.members.bulk_write(bulk_operations)
        print(f"Duplicate Detected: {result.modified_count}")

def replace_duplicate_ktp():
    pipeline = [
        {"$group": {"_id": {"$toLower": "$ktp"}, "duplicates": {"$push": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicate_ktps = list(db.members.aggregate(pipeline))

    bulk_operations = []
    for duplicate in duplicate_ktps:
        duplicates_to_null = duplicate["duplicates"]
        bulk_operations.extend([
            UpdateOne(
                {"_id": duplicate_id},
                {"$set": {"ktp": None}}
            ) for duplicate_id in duplicates_to_null[1:]
        ])

    if bulk_operations:
        result = db.members.bulk_write(bulk_operations)
        print(f"Duplicate Detected: {result.modified_count}")

def export_duplicate_email():
    duplicate_emails = db.members.aggregate([
        {"$group": {"_id": "$email", "count": {"$sum": 1}}},
        {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {"email": "$_id", "count": 1, "_id": 0}}
    ])

    csv_file_path = get_csv_path("member-email-duplikat.csv", "result")

    try:
        os.remove(csv_file_path)
    except:
        pass

    with open(csv_file_path, 'w', newline='') as csv_file:
        fieldnames = ['email', 'count']
        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
        csv_writer.writeheader()
        
        csv_writer.writerows(duplicate_emails)

def export_duplicate_ktp():
    duplicate_ktps = db.members.aggregate([
        {"$group": {"_id": "$ktp", "count": {"$sum": 1}}},
        {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {"ktp": "$_id", "count": 1, "_id": 0}}
    ])

    csv_file_path = get_csv_path("member-ktp-duplikat.csv", "result")

    try:
        os.remove(csv_file_path)
    except:
        pass

    with open(csv_file_path, 'w', newline='') as csv_file:
        fieldnames = ['ktp', 'count']
        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
        csv_writer.writeheader()
        
        csv_writer.writerows(duplicate_ktps)

def parse_email(email, code):
    if (type(email) != str or len(email) < 5):
        return f'{code.lower()}@naturalnusantara.co.id'
    else:
        return email

def parse_register_name(temp_code):
    return temp_code.split('-')[0]

def parse_ktp(ktp):
    if (ktp == ''):
        return ''
    elif (ktp == '-'):
        return ''
    return ktp

def get_csv_chunk(filename, cols):
    filepath = get_csv_path(filename)

    return pd.read_csv(filepath, chunksize=1000, encoding='latin1', keep_default_na=False, usecols=cols)

def get_csv_size(filename, cols):
    filepath = get_csv_path(filename)

    return len(pd.read_csv(filepath, encoding='latin1', usecols=cols).index)

def get_csv_path(filename, dir='files'):
    return os.path.join(os.path.dirname(__file__), f'./{dir}/{filename}')

truncate_members()
import_member_master()
import_member_bonus()
export_duplicate_email()
export_duplicate_ktp()
replace_duplicate_email()
replace_duplicate_ktp()
export_member()