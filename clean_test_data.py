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
barcodes = pd.read_excel('models/test_data_IZ_to_IZ.xlsx', sheet_name=1, dtype=str)['Barcode'].dropna().str.strip("'")
src_data = pd.read_excel('models/test_data_IZ_to_IZ.xlsx', sheet_name='Holdings', dtype=str).dropna()[['IZ_MMS_id','Holding_id']]
loans_barcodes = pd.read_excel('models/test_data_IZ_to_IZ.xlsx', sheet_name='Loans', dtype=str)['Barcode_d'].dropna().str.strip("'")
print(loans_barcodes)
# Clean loans
for barcode in loans_barcodes:
    Item(barcode=barcode, zone='ISR', env='S').scan_in(library='rro_fili', circ_desk='DEFAULT_CIRC_DESK')


# for i, barcode in enumerate(barcodes):
#     item = Item(barcode='OLD_' + barcode, zone='UBS', env='S')
#     if item.error is False:
#         item.barcode = item.barcode.replace('OLD_', '')
#         item.update()
#
# for barcode in barcodes:
#     item = Item(barcode=barcode, zone='ISR', env='S')
#     if item.error is False and item.bib is not None:
#         item.bib.delete(force=True)
#     # item.delete()
#
# for row in src_data.iterrows():
#     holding_id = row[1]['Holding_id'].strip("'")
#     mms_id = row[1]['IZ_MMS_id'].strip("'")
#     bib_s = IzBib(mms_id, zone='UBS', env='S')
#     nz_mms_id = bib_s.get_nz_mms_id()
#     if bib_s.error:
#         continue
#
#     bib_d = IzBib(nz_mms_id, zone='ISR', env='S', from_nz_mms_id=True)
#     if bib_d.error:
#         continue
#     holding_s = Holding(mms_id, holding_id, zone='UBS', env='S')
#     holdings_d = bib_d.get_holdings()
#     for holding in holdings_d:
#         if holding.callnumber == holding_s.callnumber:
#             holding.delete(force=True)
