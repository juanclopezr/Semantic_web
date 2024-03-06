from glob import glob
from pandas import read_parquet
from requests import get as get_request
from tqdm import tqdm

def download_pdf():
    """
    Función principal que gestiona la descarga de archivos PDF que aún no están presentes en la carpeta local.
    Lee un catálogo de archivos PDF desde un archivo Parquet y compara con los archivos ya descargados.
    """
    files = [f.split('/')[-1][:-4] for f in glob('./data/pdf/*.pdf')]
    
    catalogue = read_parquet('./data/api_request_results/pdf_catalogue.parquet')
    print('Total pdfs: ', len(catalogue))
    catalogue = catalogue[~catalogue['paperId'].isin(files)]
    print('Pdfs restantes: ', len(catalogue))

    urls_name = catalogue[['pdf_url', 'paperId']].values.tolist()
    
    for url, name in tqdm(urls_name):
        try:
            req = get_request(url, timeout=30)
            if req.status_code == 200:
                with open('./data/pdf/' + name + '.pdf', 'wb') as f:
                    f.write(req.content)
            else:
                raise Exception('Error')
        except Exception as e:
            print(f'Error downloading {url}')
            pass

    
    


