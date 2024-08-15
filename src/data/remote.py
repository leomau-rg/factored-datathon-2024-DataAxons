import io
import zipfile
import requests
from typing import Generator

from contextlib import closing


def download_zip(url:str) -> Generator[bytes]:
    """Descarga un archivo .zip de una url proporcionada
    
    Args
    ==========
    url (str): url del archivo zip
    
    Yields
    ==========
    bytes_data (bytes): generador de bytes de archivos zip
    
    Raises
    ==========
    ConnectionError (Error): en caso de que el estatus code sea diferente de 200
    ConnectionAbortedError (Error): si sucede algun error durante el procesamiento del archivo zip
    """
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
                
                for member in zip_files:
                    bytes_data = archive.read(member)
                    yield bytes_data
        else:
            raise ConnectionError(f"\t[ERROR]>> no se pudo establecer la conección a {url}, status code {response.status_code}")
    
    except Exception as e:
        raise ConnectionAbortedError(f"'\t[ERROR]>> error al descargar zip dess {url}: {e}")

