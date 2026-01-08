###########################
# Transfer IZ to IZ items #
###########################

# This script transfers items from IZ source to IZ destination.
# The information about the transfer should be given in an Excel file
# This file should be compliant with a given format

# To start the script:
# python transfer_iz_to_iz_items.py <dataForm.xlsx>

EXCEL_FORM_VERSION = '5.0'

import logging
import sys

from almapiwrapper.configlog import config_log
from almapiwrapper.inventory import IzBib, Holding, Item
from almapiwrapper.acquisitions import POLine, Vendor, Invoice, fetch_invoices

from utils import xlstools

import os
from dotenv import load_dotenv

# Load environment variables from .env file
if 'alma_api_keys' not in os.environ:
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    load_dotenv(dotenv_path=dotenv_path)

# Check if the correct number of arguments is provided
if len(sys.argv) != 2:
    print("Usage : python transfer_iz_to_iz_items.py <dataForm.xlsx>")
    sys.exit(1)

excel_filepath = sys.argv[1]

# Logging configuration
log_filename = xlstools.get_raw_filename(excel_filepath)
config_log(log_filename)

logging.info(f'Items transfer from IZ to IZ started: {excel_filepath}')

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
process_monitor = ProcessMonitor(excel_filepath, 'Items')

# Iterate over the PoLine numbers
for i in process_monitor.df.index:
    logging.info(f"Processing row {i} / {len(process_monitor.df.index)}: items {process_monitor.df.at[i, 'Barcode']}")
    processes.item(i)

logging.info('Items transfer from IZ to IZ terminated')

