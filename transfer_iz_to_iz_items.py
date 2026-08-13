###########################
# Transfer IZ to IZ items #
###########################

# This script transfers items from IZ source to IZ destination.
# The information about the transfer should be given in an Excel file
# This file should be compliant with a given format

# To start the script:
# python transfer_iz_to_iz_items.py <dataForm.xlsx>
from utils.settings import load_env
import logging
import sys
from almapiwrapper.configlog import config_log
from utils import xlstools, processes
from utils.processmonitoring import ProcessMonitor

def main() -> None:
    load_env()

    if len(sys.argv) != 2:
        print("Usage : python transfer_iz_to_iz_items.py <dataForm.xlsx>")
        sys.exit(1)

    excel_filepath = sys.argv[1]
    log_filename = xlstools.get_raw_filename(excel_filepath)
    config_log(log_filename)

    logging.info(f"Items transfer from IZ to IZ started: {excel_filepath}")

    xlstools.is_form_valid(excel_filepath)
    xlstools.set_config(excel_filepath)

    config = xlstools.get_config()
    process_monitor = ProcessMonitor(excel_filepath, "Items")

    nb_treatments = 0
    index_rows = process_monitor.df.loc[~process_monitor.df["Copied"].fillna(False)].index.tolist()
    for i in index_rows:
        nb_treatments += 1
        logging.info(
            f"Processing {nb_treatments} / {config['max_treatments']} - "
            f"row {i}: items {process_monitor.df.at[i, 'Barcode']}"
        )

        processes.item(i)
        if nb_treatments == config['max_treatments']:
            logging.info(f"Max number of treatments reached: {config['max_treatments']}")
            break

    logging.info("Items transfer from IZ to IZ terminated")

if __name__ == "__main__":
    main()
