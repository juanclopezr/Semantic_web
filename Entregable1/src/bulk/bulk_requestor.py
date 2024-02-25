from src.utils import retrieve_data
from pandas import read_parquet, DataFrame, concat, read_pickle
from tqdm import tqdm
from time import sleep
from glob import glob

from os import getenv

from dotenv import load_dotenv
load_dotenv()

API_KEY = getenv('API_KEY')

def request_data():
    df = read_parquet('data/data_4.parquet',
                      columns=['db_id', 'title', 'year']
                      )
    print("Original len: ", len(df))

    files = glob('data/api_request_results/documents/*')
    files = [f.split('/')[-1][5:-4] for f in files]

    df = df[~df['db_id'].isin(files)]
    df = df.sample(len(df), ignore_index=True)
    print("Actual len: ", len(df))

    for row in tqdm(df.itertuples()):

        res = retrieve_data.request_semantic_scholar(row.title,
                                                     api_key=API_KEY,
                                                     fields=retrieve_data.QUERY_FIELDS)

        if res['status_code'] == 200:
            data = res.get('data')
            if data:
                df_res = DataFrame.from_dict(data)

                df_res['db_id'] = row.db_id
                df_res['db_title'] = row.title
                df_res['db_year'] = row.year

                df_res.to_pickle(f'data/api_request_results/documents/data_{row.db_id}.zip',
                                 compression={
                                     'method': 'zip',
                                     'compresslevel': 9  # Nivel máximo de compresión para ZIP
                                 }
                                 )
            else:
                print(f'No data for {row.title} ({row.db_id})')
            sleep(1)
        elif res['status_code'] == 429:
            print(
                'Too many requests in {row.title} ({row.db_id}). Waiting 180 seconds')
            sleep(180)
        else:
            print('Error:', res)
            break


    retrieved_data = concat(
                        [read_pickle(f) for f in tqdm(glob('data/api_request_results/documents/*'))]
                        )
    
    
    retrieved_data.to_pickle(f'data/api_request_results/retrieved_data.zip', 
                             compression= {
                                'method': 'zip',
                                'compresslevel': 9  # Nivel máximo de compresión para ZIP
                                }
                        )
