from pathlib import PosixPath
from datetime import datetime as dt
from typing import Callable

import polars as pl
import pandas as pd

from src.data.remote import download_zip


def filter_by_theme(df:pl.DataFrame, theme:str) -> pl.DataFrame:
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
        pl.col(["DATE","THEMES","LOCATIONS","PERSONS","ORGANIZATIONS","SOURCEURLS"])
    )
    return df


def get_tones_and_urls(df:pl.DataFrame) -> pl.DataFrame:
    """Procesa un dataframe separando la columna TONE (scores de opinion y polaridad)
    en varias columnas (checar fuente http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf)
    y lar regresa con la fuente para obtener las noticias y hacer análisis de polaridad con vader

    Args
    ----------
    df (pl.DataFrame):
        dataframe de polars con formato del dataset gdelt-gkg

    Returns
    ----------
    df (pl.DataFrame):
        dataframe filtrado con los datos de polaridad y urls
    """
    df = df.filter(
        pl.any_horizontal(pl.col('TONE').is_not_null()),
        pl.col('TONE').str.contains(',')
    ).with_columns(pl.col('TONE').str.split(',')).filter(
        pl.col('TONE').list.len().eq(6)
    ).with_columns(
        pl.col("TONE").list.get(0).alias('general_tone'),
        pl.col("TONE").list.get(1).alias('positive_score'),
        pl.col("TONE").list.get(2).alias('negative_score'),
        pl.col("TONE").list.get(3).alias('polarity')
    ).select(
        pl.col(['SOURCEURLS', 'general_tone', 'positive_score', 'negative_score', 'polarity'])
    )

    return df


def process_gkg_csv(datafile:PosixPath, output_dir:PosixPath, limit:int, process_function:Callable, **kwargs):
    """Descarga varios los archivos zips indicados por limit de un archivo csv,
    los lee, procesa los datos como un dataframe de polars
    y los junta en un solo dataframe para al final guardarlos como un solo .parquet
    
    Args
    ==========
    datafile (pathlib.PosixPath): path del archivo csv que contiene las urls y los checksums de los archivos zip
    output_dir (pathlib.PosixPath): path del directorio donde se va a guardar el parquet resultante, si no existe se crea
    limit (int): número de archivos zip para descargar
    process_function (function): funcion de procesamiento para aplicar a cada csv descargado
    **kwargs: parámetros de la función de procesamiento 
    
    Returns
    ==========
    None"""
    
    urls_df = pd.read_csv(datafile)

    print(f'[INFO]>> saving data into {output_dir} with process {process_function.__name__}')
    output_dir.mkdir(exist_ok=True, parents=True)

    main_df = None
    
    for idx, row in urls_df.iterrows():

        url:str = row['url']
        md5:str = row['md5']
        
        if 'MASTER' in url.upper() or 'counts' in url:
            continue
        
        print(f'[INFO]>> {str(idx).rjust(4, "0")} requesting {url}...')
        try:
            for bytes_data in download_zip(url.strip()):
            
                df = pl.read_csv(bytes_data, separator='\t')
                df = process_function(df, **kwargs)

                if main_df is None:
                    main_df = df
                else:
                    main_df = main_df.vstack(df)
                    limit -= 1

        except Exception as e:
            print(f"something happened!!: {e}")
        
        if limit <= 1:
            break
        
    output_file = output_dir / (process_function.__name__ + f'{dt.now().strftime("-%y%m%d_%H%M")}.parquet')
    print(f"\t[INFO]>> saving in parket {output_file}")
    main_df.write_parquet(output_file)