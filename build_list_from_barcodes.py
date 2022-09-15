########################################################
# Build list with holding ids and mms id from barcodes #
########################################################

# To start the script:
# python build_list_from_barcodes.py <barcode_file.csv> -iz <IZ code> -s/p
# For example:
# python build_list_from_barcodes.py barcode.csv -iz ETH -s

# Import libraries
from almapiwrapper import ApiKeys
from almapiwrapper.inventory import Item
from almapiwrapper.configlog import config_log
import sys
import logging
import pandas as pd
import time

# Config logs
config_log()

if len(sys.argv) != 5:
    logging.critical('Arguments missing or incorrect')
    exit()

if sys.argv[2] != '-iz' or sys.argv[3] not in ApiKeys().get_iz_codes():
    logging.critical('Argument -iz missing or iz code incorrect')
    exit()

iz = sys.argv[3]

if sys.argv[4] == '-s':
    env = 'S'
elif sys.argv[4] == '-p':
    env = 'P'
else:
    logging.critical('Argument -p or -s missing')
    exit()

barcodes_file = sys.argv[1]

barcodes = pd.read_csv(barcodes_file)

df = pd.DataFrame(columns=['IZ MMS ID', 'NZ MMS ID', 'Holding', 'Item', 'Barcode', 'Library', 'Location', 'Process'])
for barcode in barcodes.iloc[:, 0].values:
    item = Item(barcode=barcode, zone=iz, env=env)
    df.loc[len(df)] = {'IZ MMS ID': item.bib.get_mms_id(),
                       'NZ MMS ID': item.bib.get_nz_mms_id(),
                       'Holding': item.holding.get_holding_id(),
                       'Item': item.get_item_id(),
                       'Barcode': item.barcode,
                       'Library': item.library,
                       'Location': item.location,
                       'Process': item.data.find('.//process_type').text}
    item.save()
    item.holding.save()
    item.bib.save()
    time.sleep(1)
df = df.sort_values(['NZ MMS ID', 'Holding'])
df = df.reset_index(drop=True)
df.to_csv('data/export.csv')


print(df)
