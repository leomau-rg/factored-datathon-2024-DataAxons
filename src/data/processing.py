import polars as pl


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

