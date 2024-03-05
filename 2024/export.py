import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi

mongo_uri = 'mongodb://127.0.0.1:27017'
mongo_client = MongoClient('localhost', server_api=ServerApi('1'))
db = mongo_client.nasa_import

def export_sc():
    os.system(f'mongoexport --collection=stockist_centers --db=nasa_import --type=csv --out=2024/result/sc.csv --fields=code,email,pin,stockist_code,stockist_name,member_code,member_name,name,area "{mongo_uri}"')

def export_stockist():
    os.system(f'mongoexport --collection=stockists --db=nasa_import --type=csv --out=2024/result/stockists.csv --fields=stockist_type,code,name,address,address2,phone,area,order,is_active,email,office_email,bank_name,bank_branch_name,bank_account_name,bank_account_number,cellphone,kta,pulau,period,pin,upline_code,upline_name,sc_code,sc_name,member_code,member_name "{mongo_uri}"')

def export_member():
    os.system(f'mongoexport --collection=members --db=nasa_import --type=csv --out=2024/result/members.csv --fields=code,temp_code,name,ktp,sex,birthdate,marital_status,address,address2,kta,postal_code,phone,cellphone,spouse_name,spouse_birthdate,devisor_name,devisor_birthdate,bonus_office,bank_name,bank_branch_name,upline_code,upline_name,register_code,register_name,period,npwp_number,dependents_number,bank_account_number,email,office_email,pin,stockist_code,stockist_name,sc_code,sc_name "{mongo_uri}"')

export_sc()
export_stockist()
export_member()