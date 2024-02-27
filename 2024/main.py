import os
import pandas as pd
import redis
import progressbar

redis_client = redis.Redis(host='localhost', port=6379, db=0, ssl=False)

filepath = os.path.join(os.path.dirname(__file__), './files/stockist-penjualan.csv')
chunks = pd.read_csv(filepath, chunksize=1000)
size = pd.read_csv(filepath).size

redis_pipe = redis_client.pipeline()

with progressbar.ProgressBar(max_value=size) as bar:
    for df in chunks:
        for _, row in df.iterrows():
            key = f'data:{row["KDST"]}'
            redis_pipe.hset(key, mapping={
                'code': row['KDST'],
                'name': row['NMAST'],
                'address': row['ALMST'],
                'address2': row['ALMST1'],
                'phone': row['TLPST'],
                'area': row['AREA'],
                'order': row['URUT'],
                'is_active': 1 if row['PASIF'] == 'x' else 0,
                'email': row['EMAIL']
            })
        
        redis_pipe.execute()
        bar.update(1000)