# Import libraries
from almapiwrapper.inventory import IzBib, NzBib, Holding, Item
from almapiwrapper.record import XmlData
from almapiwrapper.configlog import config_log
from almapiwrapper import ApiKeys
import pandas as pd
from copy import deepcopy
import sys
# import os
# import logging
# import openpyxl

# Config logs
config_log()

df = pd.read_csv('data/test_data_processing_save.csv', dtype=str)
df = df.replace('False', False)
df = df.replace('True', True)
df = df.replace('NaN', None)
for i, row in df.iterrows():
    item = Item(row['MMS_id_s'], row['Holding_id_s'], row['Item_id_s'], 'UBS', 'S')
    if 'OLD_' in item.barcode:
        item.barcode = item.barcode.replace('OLD_', '')
        item.update()

barcodes = df['Barcode']
for barcode in barcodes.values:
    item = Item(barcode=barcode, zone='ISR', env='S')
    if item.bib is not None:
        item.bib.delete(force=True)
    # item.delete()
