import logging
import sys

from almapiwrapper.configlog import config_log
from almapiwrapper.inventory import IzBib, Holding, Item
from almapiwrapper.acquisitions import POLine, Vendor, Invoice, fetch_invoices

from utils import xlstools, utils, polines
from utils.processmonitoring import ProcessMonitor

# Check if the correct number of arguments is provided
if len(sys.argv) != 2:
    print("Usage : python transfer_iz_to_iz_polines.py <dataForm.xlsx>")
    sys.exit(1)

excel_filepath = sys.argv[1]

# Logging configuration
log_filename = utils.get_raw_filename(excel_filepath)
config_log(log_filename)

logging.info(f'PoLines transfer from IZ to IZ started: {excel_filepath}')

# Check version of the Excel form
version = xlstools.get_form_version(excel_filepath)
if not version or not isinstance(version, str) or not version.replace('.', '', 1).isdigit():
    logging.critical(f"Invalid version format in the Excel file: {version}")
    sys.exit(1)
elif version != EXCEL_FORM_VERSION:
    logging.critical(f"Unsupported Excel form version: {version}. Expected version: {EXCEL_FORM_VERSION}")
    sys.exit(1)

# Initialize process monitor
process_monitor = ProcessMonitor(excel_filepath, 'PoLines')

# Iterate over the PoLine numbers
for i in process_monitor.df.index:
    logging.info(f"Processing row {i} / {len(process_monitor.df.index)}: PoLine number: {process_monitor.df.at[i, 'PoLine_s']}")
    polines.process(i)

logging.info('PoLines transfer from IZ to IZ terminated')

