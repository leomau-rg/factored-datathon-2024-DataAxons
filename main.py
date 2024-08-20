from pathlib import Path
from argparse import ArgumentParser

from src import scrtips

parser = ArgumentParser("Punto de entrada para procesos del challenge")
subparsers = parser.add_subparsers(dest="command")

extract_links_parser = subparsers.add_parser('extract_links')
extract_links_parser.add_argument('file', type=str, help='path al archivo html con los datos')

collect_stats_parser = subparsers.add_parser('collect_by_theme', help='recolecta un conjunto de archivos csv en un solo parquet')
collect_stats_parser.add_argument('file', type=str, help='path al archivo csv con los datos')
collect_stats_parser.add_argument('output_dir', type=str, help='directorio donde se van a guardar los datos')
collect_stats_parser.add_argument('theme', type=str, help='tema para filtrar')
collect_stats_parser.add_argument('-l', "--limit", type=int, default=5, help='número de archivos a descargar')

collect_tone_parser = subparsers.add_parser('collect_tone', help='recolecta un conjunto de archivos csv en un solo parquet filtrando solo los datos de tono')
collect_tone_parser.add_argument('file', type=str, help='path al archivo csv con los datos')
collect_tone_parser.add_argument('output_dir', type=str, help='directorio donde se van a guardar los datos')
collect_tone_parser.add_argument('-l', "--limit", type=int, default=5, help='número de archivos a descargar')

args = parser.parse_args()

if args.command == 'extract_links':
    path = Path(args.file)
    
    scrtips.data_01_extract_links.process_raw_data_links(path)

elif args.command == 'collect_by_theme':
    path = Path(args.file)
    output_dir = Path(args.output_dir)

    scrtips.data_02_collect_by_theme.run(path, output_dir, args.theme, args.limit)

elif args.command == 'collect_tone':
    path = Path(args.file)
    output_dir = Path(args.output_dir)

    scrtips.data_03_collect_with_tone.run(path, output_dir, args.limit)