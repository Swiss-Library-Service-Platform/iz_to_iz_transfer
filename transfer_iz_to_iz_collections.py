#################################
# Transfer IZ to IZ collections #
#################################

# This script add bib reocrds from collection of IZ source to the collection of
# IZ destination. The collection should already be created in the destination IZ.
# The information about the transfer should be given in an Excel file
# This file should be compliant with a given format

# To start the script:
# python transfer_iz_to_iz_holdings.py <dataForm.xlsx>

from utils.settings import load_env
import logging
import sys
from almapiwrapper.configlog import config_log
from utils import xlstools, processes
from utils.processmonitoring import ProcessMonitor


def main() -> None:
    load_env()

    if len(sys.argv) != 2:
        print("Usage : python transfer_iz_to_iz_collections.py <dataForm.xlsx>")
        sys.exit(1)

    excel_filepath = sys.argv[1]
    log_filename = xlstools.get_raw_filename(excel_filepath)
    config_log(log_filename)

    logging.info(f"Add copy of collections started: {excel_filepath}")

    xlstools.is_form_valid(excel_filepath)
    xlstools.set_config(excel_filepath)

    config = xlstools.get_config()
    process_monitor = ProcessMonitor(excel_filepath, "Collections")
    nb_treatments = 0
    index_rows = process_monitor.df.loc[~process_monitor.df["Copied"].fillna(False)].index.tolist()


    if len(index_rows) < config["max_treatments"]:
        config["max_treatments"] = len(index_rows)
    for i in index_rows:
        nb_treatments += 1
        logging.info(
            f"Processing {nb_treatments} / {config['max_treatments']} - "
            f"row {i}: "
            f"collection {process_monitor.df.at[i, 'Collection_id_s']}"
        )
        processes.collection(i)

        if nb_treatments == config["max_treatments"]:
            logging.info(f"Max number of treatments reached: {config['max_treatments']}")
            break

    logging.info("Copy collections terminated")


if __name__ == "__main__":
    main()

