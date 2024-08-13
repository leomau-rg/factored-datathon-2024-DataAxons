from pathlib import Path
from argparse import ArgumentParser

from src import scrtips

parser = ArgumentParser("Punto de entrada para procesos del challenge")
subparsers = parser.add_subparsers(dest="command")

extract_links_parser = subparsers.add_parser('extract_links')
extract_links_parser.add_argument('file', type=str, help='path al archivo html con los datos')

args = parser.parse_args()

if args.command == 'extract_links':
    path = Path(args.file)
    
    scrtips.data_01_extract_links.process_raw_data_links(path)
