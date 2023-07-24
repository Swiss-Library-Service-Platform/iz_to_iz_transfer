# Import libraries
from almapiwrapper.inventory import Item
from almapiwrapper.configlog import config_log
import pandas as pd

# import openpyxl

# Config logs
config_log()
barcodes = pd.read_excel('data/test_data.xlsx', sheet_name=1, dtype=str)['Barcode'].dropna().str.strip("'")

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
