import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
import progressbar

mongo_uri = 'mongodb://127.0.0.1:27017'
mongo_client = MongoClient('localhost', server_api=ServerApi('1'))
db = mongo_client.nasa_import

def truncate_sc():
    db.stockist_centers.delete_many({})

def import_sc():
    print('Importing raw sc')

    filesize = get_csv_size('sc.csv')
    chunk_size = min(filesize, 1000)
    chunks = get_csv_chunk('sc.csv', chunk_size)

    with progressbar.ProgressBar(max_value=filesize) as bar:
        for df in chunks:
            stockists = list(db.stockists.find({
                'code': {'$in': df['KODEST'].tolist()}
            }))
            members = list(db.members.find({
                'code': {'$in': [document['member_code'] for document in stockists if 'member_code' in document]}
            }))
            stockist_hash = {item['code']: {**item} for item in stockists if 'code' in item}
            member_hash = {item['code']: {**item} for item in members if 'code' in item}

            payload_sc = []
            payload_stockist = []
            payload_member = []
            for _, row in df.iterrows():
                def get_stockist(key):
                    if (not (row['KODEST'] in stockist_hash)):
                        return None
                    
                    if (not (key in stockist_hash[row['KODEST']])):
                        return None
                    
                    return stockist_hash[row['KODEST']][key]

                def get_member(key):
                    member_code = get_stockist('member_code')

                    if (not member_code):
                        return None

                    if (not member_code in member_hash):
                        return None
                    
                    if (not (key in member_hash[member_code])):
                        return None
                    
                    return member_hash[member_code][key]
                
                payload_sc.append(UpdateOne(
                    {'code': row['KODESC']},
                    {'$set': {
                        'code': row['KODESC'],
                        'email': f'{row["KODESC"]}@naturalnusantara.co.id',
                        'stockist_code': get_stockist('code'),
                        'stockist_name': get_stockist('name'),
                        'pin': get_stockist('pin'),
                        'member_code': get_member('code'),
                        'member_name': get_member('name'),
                        'name': row['NMASC'],
                        'area': row['WILSC']
                    }},
                    upsert=True
                ))

                payload_stockist.append(UpdateOne(
                    {'code': row['KODEST']},
                    {'$set': {
                        'sc_code': row['KODESC'],
                        'sc_name': row['NMASC']
                    }}
                ))

                if (get_member('code')):
                    payload_member.append(UpdateOne(
                        {'code': get_member('code')},
                        {'$set': {
                            'sc_code': row['KODESC'],
                            'sc_name': row['NMASC']
                        }}
                    ))

            db.stockist_centers.bulk_write(payload_sc)
            db.stockists.bulk_write(payload_stockist)
            
            if (payload_member):
                db.members.bulk_write(payload_member)

            bar.update(chunk_size)

def get_csv_chunk(filename, chunk_size):
    filepath = get_csv_path(filename)

    return pd.read_csv(filepath, chunksize=chunk_size, encoding='latin1', keep_default_na=False)

def get_csv_size(filename):
    filepath = get_csv_path(filename)

    return len(pd.read_csv(filepath, encoding='latin1').index)

def get_csv_path(filename):
    return os.path.join(os.path.dirname(__file__), f'./files/{filename}')

def export_sc():
    os.system(f'mongoexport --collection=stockist_centers --db=nasa_import --type=csv --out=2024/result/sc.csv --fields=code,email,pin,stockist_code,stockist_name,member_code,member_name,name,area "{mongo_uri}"')

truncate_sc()
import_sc()
export_sc()