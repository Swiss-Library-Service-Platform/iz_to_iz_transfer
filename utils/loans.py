import logging
from typing import Optional

import pandas as pd
from almapiwrapper.inventory import Item
from almapiwrapper.users import User, Loan

from utils import xlstools
from utils.processmonitoring import ProcessMonitor

config = xlstools.get_config()


def create_loan(i: int) -> Optional[Loan]:
    """
    Creates a loan for an item based on the index provided in the DataFrame.
    This function retrieves the source item using the MMS ID, Holding ID, and Item ID from the DataFrame,
    and attempts to loan it out at the specified circulation desk in the source IZ.

    Parameters
    ----------
    i : int
        The index of the row in the process monitor DataFrame to process.

    Returns
    -------
    Optional[Loan]
        The Loan object if the loan is successful, or None if an error occurs.
    """
    process_monitor = ProcessMonitor()

    mms_id_d = process_monitor.df.at[i, 'MMS_id_d']
    holding_id_d = process_monitor.df.at[i, 'Holding_id_d']
    item_id_d = process_monitor.df.at[i, 'Item_id_d']
    primary_id = process_monitor.df.at[i, 'Primary_id']
    barcode_s = process_monitor.df.at[i, 'Barcode_s']
    barcode_d = process_monitor.df.at[i, 'Barcode_d']

    # Case when all IDs are provided in the destination IZ
    if pd.notnull(mms_id_d) and pd.notnull(holding_id_d) and pd.notnull(item_id_d):
        item_d = Item(mms_id_d, holding_id_d, item_id_d, zone=config['iz_d'], env=config['env'])

    # Case when barcode only is provided in the destination IZ
    elif pd.notnull(barcode_d):
        item_d = Item(barcode=barcode_d, zone=config['iz_d'], env=config['env'])

    # Case when barcode only of source item is provided
    elif (pd.isnull(mms_id_d) or pd.isnull(holding_id_d) or pd.isnull(item_id_d)) and pd.notnull(barcode_s):
        # If the source barcode is set, we use it to find the item in the destination IZ
        item_d = Item(barcode=barcode_s.replace('OLD_', ''), zone=config['iz_d'], env=config['env'])

    else:
        logging.error(f"Row {i}: Expected item information missing")
        process_monitor.df.at[i, 'Error'] = 'Expected item information missing'
        process_monitor.save()
        return None

    _ = item_d.data  # Fetch the item data
    if item_d.error:
        logging.error(f"{repr(item_d)}: {item_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source Item not found'
        process_monitor.save()
        return None

    library_d = item_d.library

    if item_d.error:
        logging.error(f"{repr(item_d)}: {item_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source Item not found'
        process_monitor.save()
        return None

    user_d = User(primary_id, zone=config['iz_d'], env=config['env'])

    # Attempt to create a loan
    loan = user_d.create_loan(item=item_d, library=library_d, circ_desk=config['circ_desk_d'])

    return loan


def make_return(i: int) -> Optional[Item]:
    """
    Makes a return of an item based on the index provided in the DataFrame.
    This function retrieves the source item using the MMS ID, Holding ID, and Item ID from the DataFrame,
    and attempts to return it at the specified circulation desk in the source IZ.

    Parameters
    ----------
    i : int
        The index of the row in the process monitor DataFrame to process.

    Returns
    -------
    Optional[Item]
        The Item object if the return is successful, or None if an error occurs.
    """
    process_monitor = ProcessMonitor()

    # If the source barcode is not set, we make a return
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']
    item_id_s = process_monitor.df.at[i, 'Item_id_s']
    barcode_s = process_monitor.df.at[i, 'Barcode_s']

    # If the source barcode is set, we use it to find the item in the source IZ
    if pd.notnull(barcode_s):
        item_s = Item(barcode=barcode_s, zone=config['iz_s'], env=config['env'])
        _ = item_s.data
        if item_s.error and 'OLD_' in barcode_s:
            item_s = Item(barcode=barcode_s.replace('OLD_', ''), zone=config['iz_s'], env=config['env'])
            _ = item_s.data
        if item_s.error:
            logging.error(f"{repr(item_s)}: {item_s.error_msg}")
            process_monitor.df.at[i, 'Error'] = 'Source Item not found'
            process_monitor.save()
            return None
    elif pd.notnull(mms_id_s) and pd.notnull(holding_id_s) and pd.notnull(item_id_s):
        # Create item object using MMS ID, Holding ID, and Item ID
        item_s = Item(mms_id_s, holding_id_s, item_id_s, zone=config['iz_s'], env=config['env'])

    else:
        logging.error(f"Row {i}: Expected item information missing")
        process_monitor.df.at[i, 'Error'] = 'Expected item information missing'
        process_monitor.save()
        return None

    # Return item
    library_s = item_s.library

    if item_s.error:
        logging.error(f"{repr(item_s)}: {item_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source Item not found'
        process_monitor.save()
        return None

    item_s = item_s.scan_in(library=library_s, circ_desk=config['circ_desk_s'])

    return item_s
