from pathlib import PosixPath

import pandas as pd
from polars import read_csv

from src.data.remote import download_zip
from src.data.processing import filter_by_theme


def run(datafile:PosixPath, output_dir:PosixPath, keyword:str='HEALTH', limit:int=100) -> None:
    """Descarga varios archivos zips de un archivo csv los lee, procesa los datos como un dataframe de polars
    y los junta en un solo dataframe para al final guardarlos como un solo .parquet
    
    Args
    ==========
    datafile (pathlib.PosixPath): path del archivo csv que contiene las urls y los checksums de los archivos zip
    output_dir (pathlib.PosixPath): path del directorio donde se va a guardar el parquet resultante, si no existe se crea
    keyword (str): cadena para filtrar en la columna 'THEMES' de los csv descargados
    limit (int): número de archivos zip para descargar
    
    Returns
    ==========
    None"""
    
    urls_df = pd.read_csv(datafile)

    print(f'[INFO]>> saving data into {output_dir}')
    output_dir.mkdir(exist_ok=True, parents=True)

    main_df = None
    
    # los rows contienen los headers ['url', 'md5']
    for idx, row in urls_df.iterrows():

        url:str = row['url']
        md5:str = row['md5']
        
        if 'MASTER' in url.upper() or 'counts' in url:
            continue
        
        print(f'[INFO]>> {str(idx).rjust(4, "0")} requesting {url}...')
        try:
            for bytes_data in download_zip(url.strip()):
            
                df = read_csv(bytes_data, separator='\t')
                df = filter_by_theme(df, keyword)

                if main_df is None:
                    main_df = df
                else:
                    main_df = main_df.vstack(df)
                    limit -= 1

        except Exception as e:
            print(f"something happened!!: {e}")
        
        if limit <= 1:
            break
        
    output_file = output_dir / (keyword + '.parquet')
    print(f"\t[INFO]>> saving in parket {output_file}")
    main_df.write_parquet(output_file)

