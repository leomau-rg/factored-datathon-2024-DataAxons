from pathlib import PosixPath

from src.data.processing import get_tones_and_urls, process_gkg_csv


def run(datafile:PosixPath, output_dir:PosixPath, limit:int=100) -> None:
    """Descarga varios archivos zips de un archivo csv los lee, procesa los datos como un dataframe de polars
    y los junta en un solo dataframe para al final guardarlos como un solo .parquet
    
    Args
    ==========
    datafile (pathlib.PosixPath): path del archivo csv que contiene las urls y los checksums de los archivos zip
    output_dir (pathlib.PosixPath): path del directorio donde se va a guardar el parquet resultante, si no existe se crea
    limit (int): número de archivos zip para descargar
    
    Returns
    ==========
    None"""

    process_gkg_csv(datafile, output_dir, limit, get_tones_and_urls)
