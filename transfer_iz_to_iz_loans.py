###########################
# Transfer IZ to IZ loans #
###########################

# This script transfers loans from IZ source to IZ destination.
# The information about the transfer should be given in an Excel file

# To start the script:
# python transfer_iz_to_iz_loans.py <dataForm.xlsx>

EXCEL_FORM_VERSION = '5.0'

import logging
import sys

from almapiwrapper.configlog import config_log
from almapiwrapper.inventory import IzBib, Holding, Item
from almapiwrapper.acquisitions import POLine, Vendor, Invoice, fetch_invoices
import pandas as pd

from utils import xlstools

# Check if the correct number of arguments is provided
if len(sys.argv) != 2:
    print("Usage : python transfer_iz_to_iz_loans.py <dataForm.xlsx>")
    sys.exit(1)

excel_filepath = sys.argv[1]

# Logging configuration
log_filename = xlstools.get_raw_filename(excel_filepath)
config_log(log_filename)

logging.info(f'Loans transfer from IZ to IZ started: {excel_filepath}')

# Check version of the Excel form
version = xlstools.get_form_version(excel_filepath)
if not version or not isinstance(version, str) or not version.replace('.', '', 1).isdigit():
    logging.critical(f"Invalid version format in the Excel file: {version}")
    sys.exit(1)
elif version != EXCEL_FORM_VERSION:
    logging.critical(f"Unsupported Excel form version: {version}. Expected version: {EXCEL_FORM_VERSION}")
    sys.exit(1)

# load configuration
xlstools.set_config(excel_filepath)

from utils import processes
from utils.processmonitoring import ProcessMonitor

# Initialize process monitor
process_monitor = ProcessMonitor(excel_filepath, 'Loans')

# Iterate over the PoLine numbers
for i in process_monitor.df.index:
    if pd.notnull(process_monitor.df.at[i, 'Item_id_s']):
        logging.info(f"Processing row {i} / {len(process_monitor.df.index)}: circulation operation on item {process_monitor.df.at[i, 'Item_id_s']}")
    elif pd.notnull(process_monitor.df.at[i, 'Barcode_s']):
        logging.info(f"Processing row {i} / {len(process_monitor.df.index)}: circulation operation on item {process_monitor.df.at[i, 'Barcode_s']}")
    elif pd.notnull(process_monitor.df.at[i, 'Item_id_d']):
        logging.info(f"Processing row {i} / {len(process_monitor.df.index)}: circulation operation on item {process_monitor.df.at[i, 'Item_id_d']}")
    else:
        logging.info(f"Processing row {i} / {len(process_monitor.df.index)}: circulation operation without item information")
    processes.loan(i)

logging.info('Loans transfer from IZ to IZ terminated')
