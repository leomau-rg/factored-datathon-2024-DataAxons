import io
import zipfile
import requests
from pathlib import PosixPath

import polars as pl
import pandas as pd
from contextlib import closing


def process_df(df:pl.DataFrame, theme:str) -> pl.DataFrame:
    """Procesa un dataframe filtrando por tema y acotando las columnas a algunas de interés
    
    Args
    ==========
    df (polars.DataFrame): dataframe de polars
    theme (str): cadena para filtrar en la columna 'THEMES'
    
    Returns
    ==========
    df (polars.DataFrame): dataframe de polars"""

    df = df.filter(
        pl.col('THEMES').str.contains(theme)
    ).select(
        pl.col("DATE"),
        pl.col("THEMES"),
        pl.col("LOCATIONS"),
        pl.col("PERSONS"),
        pl.col("ORGANIZATIONS"),
        pl.col("SOURCEURLS")
    )
    return df


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

        url = row['url']
        md5 = row['md5']
        
        if 'MASTER' in url.upper() or 'counts' in url:
            continue
        
        print(f'[INFO]>> {str(idx).rjust(4, "0")} requesting {url}...')
        try:
            response = requests.get(url.strip(), stream=True, timeout=10)
            if response.status_code == 200:
                # TODO: verificar checksum md5
                
                print(f'\t[INFO]>> reading zip file...')
                with closing(response), zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                    zip_files = archive.infolist()
                    # si hay más de un archivo solo se agarra el primero
                    if len(zip_files) > 1:
                        print(f'\t[WARNING]>> el zip de la url {url} tiene más de un archivo')

                    print(f'\t[INFO]>> processing...')
                    for member in zip_files:
                        bytes_data = archive.read(member)
                        df = pl.read_csv(bytes_data, separator='\t')
                        df = process_df(df, keyword)

                        if main_df is None:
                            main_df = df
                        else:
                            main_df = main_df.vstack(df)
                            limit -= 1
            else:
                print(f'[WARNING]>> no se pudo descargar el archivo')
        except Exception as e:
            print(f"something happened!!: {e}")
        if limit <= 1:
            break
        
    print("saving everything in parket...")
    main_df.write_parquet(output_dir / (keyword + '.parquet'))

