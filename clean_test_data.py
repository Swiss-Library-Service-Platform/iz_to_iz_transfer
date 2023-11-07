# Import libraries
from almapiwrapper.inventory import Item, Holding
from almapiwrapper.configlog import config_log
import pandas as pd

# import openpyxl

# Config logs
config_log()
barcodes = pd.read_excel('data/test_data.xlsx', sheet_name=1, dtype=str)['Barcode'].dropna().str.strip("'")
src_data = pd.read_excel('data/test_data.xlsx', sheet_name='Holdings', dtype=str).dropna()[['IZ_MMS_id','Holding_id']]


for i, barcode in enumerate(barcodes):
    item = Item(barcode='OLD_' + barcode, zone='UBS', env='S')
    if item.error is False:
        item.barcode = item.barcode.replace('OLD_', '')
        item.update()

for barcode in barcodes:
    item = Item(barcode=barcode, zone='ISR', env='S')
    if item.error is False and item.bib is not None:
        item.bib.delete(force=True)
    # item.delete()

for row in src_data.iterrows():
    holding_id = row[1]['Holding_id'].strip("'")
    mms_id = row[1]['IZ_MMS_id'].strip("'")
    holding = Holding(holding_id=holding_id, mms_id=mms_id, zone='ISR', env='S')
    holding.delete(force=True)
