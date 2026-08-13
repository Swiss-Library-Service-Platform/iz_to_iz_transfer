# Import libraries
from almapiwrapper.inventory import Item, Holding, IzBib
from almapiwrapper.configlog import config_log
import pandas as pd

import os
from dotenv import load_dotenv

# Load environment variables from .env file
if 'alma_api_keys' not in os.environ:
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    load_dotenv(dotenv_path=dotenv_path)

# import openpyxl

# Config logs
config_log()
loans_barcodes = pd.read_excel('models/test_data_IZ_to_IZ.xlsx', sheet_name='Loans', dtype=str)['Barcode_d'].dropna().str.strip("'")
print(loans_barcodes)
# Clean loans

for barcode in loans_barcodes:
    Item(barcode=barcode, zone='ISR', env='S').scan_in(library='rro_fili', circ_desk='DEFAULT_CIRC_DESK')
