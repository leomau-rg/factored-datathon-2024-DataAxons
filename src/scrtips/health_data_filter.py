import polars as pl
import matplotlib.pyplot as plt
import sys
from typing import Any

def configure_output_encoding() -> None:
    """Ensure UTF-8 encoding for the standard output."""
    sys.stdout.reconfigure(encoding='utf-8')

def read_csv(file_path: str, separator: str = '\t') -> pl.DataFrame:
    """Read a CSV file into a Polars DataFrame.
    
    Args:
        file_path (str): The path to the CSV file.
        separator (str): The separator used in the CSV file.
    
    Returns:
        pl.DataFrame: The DataFrame containing the CSV data.
    """
    return pl.read_csv(file_path, separator=separator)

def filter_health_data(df: pl.DataFrame) -> pl.DataFrame:
    """Filter the DataFrame for rows containing 'HEALTH' in the 'THEMES' column.
    
    Args:
        df (pl.DataFrame): The original DataFrame.
    
    Returns:
        pl.DataFrame: The filtered DataFrame.
    """
    return df.filter(pl.col("THEMES").str.contains("HEALTH"))

def get_source_shape_counts(df: pl.DataFrame) -> pl.DataFrame:
    """Group by 'SOURCES' and count the occurrences.
    
    Args:
        df (pl.DataFrame): The filtered DataFrame.
    
    Returns:
        pl.DataFrame: The DataFrame with source shape counts.
    """
    return (
        df.group_by("SOURCES")
        .agg(pl.count("SOURCES").alias("count"))
        .sort("count", descending=True)
    )

def save_to_csv(df: pl.DataFrame, file_path: str, separator: str = '\t') -> None:
    """Save the DataFrame to a CSV file.
    
    Args:
        df (pl.DataFrame): The DataFrame to save.
        file_path (str): The path to the CSV file.
        separator (str): The separator to use in the CSV file.
    """
    df.write_csv(file_path, separator=separator)

def plot_top_sources(df: pl.DataFrame, top_n: int = 50) -> None:
    """Plot the top N frequent sources.
    
    Args:
        df (pl.DataFrame): The DataFrame with source shape counts.
        top_n (int): The number of top sources to plot.
    """
    top_freq = df.head(top_n)
    plt.figure(figsize=(10, 6))
    plt.barh(top_freq["SOURCES"], top_freq["count"], color='skyblue')
    plt.xlabel('Count')
    plt.ylabel('Source')
    plt.title('Count of Rows by Source Shape')
    plt.gca().invert_yaxis()
    plt.show()

def main() -> None:
    """Main function to execute the script."""
    configure_output_encoding()
    
    df = read_csv("20240812.gkg.csv")
    health_df = filter_health_data(df)
    source_shape_counts = get_source_shape_counts(health_df)
    
    print(source_shape_counts)
    
    df = df.join(source_shape_counts.head(50), on="SOURCES", how="inner")
    save_to_csv(df, "updated_20240812.gkg.csv")
    
    plot_top_sources(source_shape_counts)

if __name__ == "__main__":
    main()
