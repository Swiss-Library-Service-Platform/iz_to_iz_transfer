# Import libraries
from almapiwrapper.inventory import Item, Holding, IzBib
from almapiwrapper.configlog import config_log
import pandas as pd
from utils.settings import load_env
import os
from pymongo import MongoClient
from lxml import etree

load_env()
uri = os.environ.get("MONGODB_URI_IZ_TO_IZ")

# import openpyxl

# Config logs
config_log()
barcodes = pd.read_excel('models/test_data_IZ_to_IZ.xlsx', sheet_name=1, dtype=str)['Barcode'].dropna().str.strip("'")

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

src_data = pd.read_excel('models/test_data_IZ_to_IZ.xlsx', sheet_name='Holdings', dtype=str).dropna()[['IZ_MMS_id','Holding_id']]

for row in src_data.iterrows():
    holding_id = row[1]['Holding_id'].strip("'")
    mms_id = row[1]['IZ_MMS_id'].strip("'")
    bib_s = IzBib(mms_id, zone='UBS', env='S')
    nz_mms_id = bib_s.get_nz_mms_id()
    if bib_s.error:
        continue

    bib_d = IzBib(nz_mms_id, zone='ISR', env='S', from_nz_mms_id=True)
    if bib_d.error:
        continue
    holding_s = Holding(mms_id, holding_id, zone='UBS', env='S')
    holdings_d = bib_d.get_holdings()
    for holding in holdings_d:
        if holding.callnumber == holding_s.callnumber:
            holding.delete(force=True)

src_data = pd.read_excel('models/test_data_IZ_to_IZ.xlsx', sheet_name='Bibs', dtype=str).dropna()[['IZ_MMS_id']]

for row in src_data.iterrows():
    mms_id = row[1]['IZ_MMS_id'].strip("'")
    bib_s = IzBib(mms_id, zone='UBS', env='S')
    nz_mms_id = bib_s.get_nz_mms_id()
    if bib_s.error:
        continue

    bib_d = IzBib(nz_mms_id, zone='ISR', env='S', from_nz_mms_id=True).delete(force=True)


# Ensure expected local 990$a values exist for the target UBS bib used in tests.
target_mms_id = '9972994698105504'
target_bib = IzBib(target_mms_id, zone='UBS', env='S')

if target_bib.error:
    print(f"Unable to load bib {target_mms_id} in UBS: {target_bib.error_msg}")
else:
    existing_990a = {
        (subfield.text or '').strip()
        for subfield in target_bib.data.findall('.//datafield[@tag="990"]/subfield[@code="a"]')
        if subfield.text is not None
    }
    required_990a = {'ofjnewmono', 'ofjdon'}
    missing_990a = sorted(required_990a - existing_990a)

    if not missing_990a:
        print(f"Bib {target_mms_id}: 990$$a already contains {sorted(required_990a)}")
    else:
        record = target_bib.data.find('.//record')
        if record is None:
            print(f"Bib {target_mms_id}: MARC record node not found")
        else:
            for value in missing_990a:
                datafield_990 = etree.Element('datafield', tag='990', ind1=' ', ind2=' ')
                subfield_a = etree.SubElement(datafield_990, 'subfield', code='a')
                subfield_a.text = value
                subfield_9 = etree.SubElement(datafield_990, 'subfield', code='9')
                subfield_9.text = 'LOCAL'
                record.append(datafield_990)

            target_bib.sort_fields().update()
            if target_bib.error:
                print(f"Bib {target_mms_id}: failed to update 990$$a values ({target_bib.error_msg})")
            else:
                print(f"Bib {target_mms_id}: added missing 990$$a values: {missing_990a}")


if not uri:
    raise SystemExit("Missing env var MONGODB_URI_IZ_TO_IZ")

db_name = "ISR_rro_fili"
collections_to_drop = [
    "ISR_rro_fili_Bibs_processing",
    "ISR_rro_fili_Items_processing",
]

client = MongoClient(
    uri,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=5000,
    socketTimeoutMS=10000,
    retryWrites=True,
)
db = client[db_name]

for col_name in collections_to_drop:
    existed = col_name in db.list_collection_names()
    db.drop_collection(col_name)
    print(f"{col_name}: {'dropped' if existed else 'not found (nothing to drop)'}")



client.close()