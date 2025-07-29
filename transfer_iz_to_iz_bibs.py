##########################
# Transfer IZ to IZ bibs #
##########################

# This script transfers bib reocrds from IZ source to IZ destination.
# The information about the transfer should be given in an Excel file
# This file should be compliant with a given format

# To start the script:
# python transfer_iz_to_iz_holdings.py <dataForm.xlsx>

EXCEL_FORM_VERSION = '5.0'

import logging
import sys

from almapiwrapper.configlog import config_log
from almapiwrapper.inventory import IzBib, Holding, Item
from almapiwrapper.acquisitions import POLine, Vendor, Invoice, fetch_invoices

from utils import xlstools

# Check if the correct number of arguments is provided
if len(sys.argv) != 2:
    print("Usage : python transfer_iz_to_iz_bibs.py <dataForm.xlsx>")
    sys.exit(1)

excel_filepath = sys.argv[1]

# Logging configuration
log_filename = xlstools.get_raw_filename(excel_filepath)
config_log(log_filename)

logging.info(f'Bib records transfer from IZ to IZ started: {excel_filepath}')

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
process_monitor = ProcessMonitor(excel_filepath, 'Bibs')

# Iterate over the PoLine numbers
for i in process_monitor.df.index:
    logging.info(f"Processing row {i} / {len(process_monitor.df.index)}: bib record {process_monitor.df.at[i, 'MMS_id_s']}")
    processes.bib(i)

logging.info('Bib records transfer from IZ to IZ terminated')

