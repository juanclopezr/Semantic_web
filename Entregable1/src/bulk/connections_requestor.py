from src.utils.retrieve_data import QUERY_FIELDS
from pandas import read_pickle, DataFrame, concat
from tqdm import tqdm
from thefuzz import fuzz

from requests import post as post_request

from glob import glob

from time import sleep

from dotenv import load_dotenv
from os import getenv

load_dotenv()


API_KEY = getenv('API_KEY')


def chunk_list(input_list, chunk_size):
    """Divide una lista en sub-listas de tamaño chunk_size.

    Args:
        input_list (list): Lista original que se va a dividir.
        chunk_size (int): Tamaño deseado para las sub-listas.

    Returns:
        list of lists: Lista de sub-listas, donde cada sub-lista tiene un tamaño de hasta chunk_size.
    """
    # Usar una comprensión de lista para generar las sub-listas
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]


def create_connections_catalogue():
    
    df = read_pickle("data/api_request_results/retrieved_data.zip").reset_index(drop=True)
    
    df['simi_score'] = [fuzz.partial_ratio(row.title.strip().lower(), row.db_title.strip().lower()) for row in df.itertuples()]
    df.sort_values(['db_id','simi_score'], inplace=True)
    df.drop_duplicates(['db_id'], keep='last', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    # Realmente queremos quedarnos unicamente con los papers del dataset?
    df_filter = df.query('simi_score>85') # hay falsos positivos, pero no muchos

    # O unicamente con los papers que son de acceso abierto (pdf descargable)
    df_filter = df_filter[df_filter.isOpenAccess].reset_index(drop=True)

    del df
    # usar todo => más datos!
    
    dfs = []
    for row in tqdm(df_filter.itertuples()):
        
        refs_df = DataFrame(row.references)
        refs_df['db_id'] = row.db_id
        refs_df['con_type'] = 'reference'
        
        cits_df = DataFrame(row.citations)
        cits_df['db_id'] = row.db_id
        cits_df['con_type'] = 'citation'
        
        con_df = concat([refs_df, cits_df], ignore_index=True)
        
        dfs.append(con_df)
        
    cons_df = concat(dfs, ignore_index=True)    
    cons_df.drop_duplicates('paperId', inplace=True)
    cons_df['isOpenAccess'] = cons_df.isOpenAccess.fillna(False)

    cons_df = cons_df[cons_df.isOpenAccess]
    cons_df = cons_df.sample(len(cons_df)).reset_index(drop=True)
    
    cons_df.to_pickle("data/api_request_results/connections_catalogue.zip",
                      compression={
                                'method': 'zip',
                                'compresslevel': 9  
                            })
    
    
def request_data(chunk_size=3):
    cons_df = read_pickle('data/api_request_results/connections_catalogue.zip')
    
    print("Original len: ", len(cons_df))
    
    try:
        already_cons = read_pickle('data/api_request_results/retrieved_data_connections.zip')
    except FileNotFoundError:
        already_cons = DataFrame(columns=cons_df.columns)
        
    df = cons_df[~cons_df['paperId'].isin(already_cons['paperId'])]
    df = df.sample(len(df), ignore_index=True)
    print("Actual len: ", len(df))

    del already_cons
    del cons_df

    sub_lists = chunk_list(df['paperId'].to_list(), chunk_size=chunk_size)

    for chunk in tqdm(sub_lists):

        res = post_request(
            'https://api.semanticscholar.org/graph/v1/paper/batch',
            params={'fields': QUERY_FIELDS},
            json={"ids": chunk},
            headers={'x-api-key': API_KEY} if API_KEY else {}
            )
        
        if res.status_code == 200:
            data = res.json()

            df_res = DataFrame.from_dict(data)

            df_res.to_pickle(f'data/api_request_results/connections/chunk_{df_res.paperId.iloc[0]}.zip',
                            compression={
                                'method': 'zip',
                                'compresslevel': 9  # Nivel máximo de compresión para ZIP
                            }
                            )
                
            sleep(2)
        elif res.status_code == 429:
            print('Too many requests. Waiting 180 seconds')
            sleep(180)
        else:
            try:
                msn = res.json()['error']
                
                if "maximum size" in msn:
                    print('Maximum size reached. Waiting 10 seconds')
                    sleep(10)
                    continue
                else:
                    print('Error:', msn)
                    break
            except:
                print('Error:', res.status_code)
                break
    
    retrieved_data = concat(
                    [read_pickle(f) for f in tqdm(glob('data/api_request_results/connections/*'))]
                    )


    retrieved_data.to_pickle('data/api_request_results/retrieved_data_connections.zip', 
                                compression= {
                                'method': 'zip',
                                'compresslevel': 9  # Nivel máximo de compresión para ZIP
                                }
                        )
    
    del retrieved_data
