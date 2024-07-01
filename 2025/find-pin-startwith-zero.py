import os
import pandas as pd

def find_pin_start_with_zero():
    print('Check member bonus')

    save_file_path = get_csv_path("member-pin-startswith-zero.csv", "result")

    try:
        os.remove(save_file_path)
    except:
        pass

    cols = ['NMRDST','KDEDST','NMADST','TGLLHR','NMRUPL','NMAUPL', 'PIN']
    output_cols = ['KDEDST', 'PIN']

    chunks = get_csv_chunk('member-bonus.csv', cols)

    result = []

    for df in chunks:
        df['PIN'] = df['PIN'].astype(str)
        mask = df['PIN'].str.startswith('0')
        filtered_df = df[mask][output_cols]
        
        result.append(filtered_df)

    result_df = pd.concat(result)
    result_df = result_df.rename(columns={'KDEDST': 'kode', 'PIN': 'pin'})

    result_df.to_csv(save_file_path, index=False)

def get_csv_chunk(filename, cols):
    filepath = get_csv_path(filename)

    return pd.read_csv(filepath, chunksize=1000, encoding='latin1', keep_default_na=False, usecols=cols, delimiter=',')

def get_csv_path(filename, dir='files'):
    return os.path.join(os.path.dirname(__file__), f'./{dir}/{filename}')

find_pin_start_with_zero()