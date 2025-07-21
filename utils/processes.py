import os
from utils import polines, bibs, holdings, items, xlstools
from utils.processmonitoring import ProcessMonitor
import logging
import pandas as pd
from almapiwrapper.inventory import IzBib

config = xlstools.get_config()


def poline(i: int) -> None:
    """
    Processes a single row in the process monitor DataFrame.

    Parameters
    ----------
    i : int
        The index of the row to process.

    Returns
    -------
    None
    """
    process_monitor = ProcessMonitor()

    # Check if the row is already copied
    if process_monitor.df.at[i, 'Copied']:
        # If the row is already copied, we skip it
        return None

    # Get the source PoLine number, MMS ID, Holding ID, and Item ID
    pol_number_s = process_monitor.df.at[i, 'PoLine_s']
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']
    item_id_s = process_monitor.df.at[i, 'Item_id_s']

    pol_number_d, pol_purchase_type = process_monitor.get_corresponding_poline(pol_number_s)
    if pol_number_d is None:
        polines.copy_poline(i)

    # The corresponding MMS ID in the destination IZ should exist now
    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)
    pol_number_d, pol_purchase_type = process_monitor.get_corresponding_poline(pol_number_s)

    # If the destination PoLine number or MMS ID is None, we cannot proceed
    if mms_id_d is None or pol_number_d is None:
        return None

    if process_monitor.get_corresponding_holding_id(holding_id_s) is None:
        # Copy data from the source holding from to the destination IZ
        holdings.copy_holding_data(mms_id_s, holding_id_s, mms_id_d)

    # Check if the holding could be retrieved
    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)
    if holding_id_d is None:
        return None

    if pd.isna(item_id_s):
        # If the item ID is NaN, we skip the item processing
        logging.warning(f"Item ID is NaN for row {i}, skipping item processing.")
        process_monitor.df.at[i, 'Copied'] = True
        process_monitor.save()
        return None

    # Copy the source item into the destination IZ
    items.copy_item_to_destination_iz(i, poline=True)


def item(i: int) -> None:
    """
    Processes all items in the process monitor DataFrame.

    This function iterates through each row in the process monitor DataFrame,
    checks if the item has already been copied, and if not, processes it.
    """
    process_monitor = ProcessMonitor()

    if process_monitor.df.at[i, 'Copied']:
        # If the row is already copied, we skip it
        return None


    # ----------------------------------------
    # Retrieve the source item and its details
    # ----------------------------------------

    item_s = items.get_source_item(i)
    if item_s is None:
        # If the source item could not be retrieved, we skip the row
        return None

    item_id_s = item_s.get_item_id()
    holding_id_s = item_s.get_holding_id()
    iz_mms_id_s = item_s.get_mms_id()
    nz_mms_id = item_s.get_nz_mms_id()

    process_monitor.df.at[i, 'Item_id_s'] = item_id_s
    process_monitor.df.at[i, 'Holding_id_s'] = holding_id_s
    process_monitor.df.at[i, 'MMS_id_s'] = iz_mms_id_s
    process_monitor.save()


    # --------
    # Copy bib
    # --------
    bib_d = None
    mms_id_d = process_monitor.get_corresponding_mms_id(iz_mms_id_s)

    # Case if we don't have a destination known MMS ID
    if mms_id_d is None:
        bib_d = bibs.copy_bib_from_nz_to_dest_iz(iz_mms_id_s)

        if bib_d is None:
            # If the destination bib could not be created, we skip the row
            return None

    # ------------
    # Copy holding
    # ------------
    if process_monitor.get_corresponding_holding_id(holding_id_s) is None:
        # Copy the holding data from the source to the destination IZ
        holding_d = holdings.copy_holding_to_destination_iz(i, bib_d)

    # -------------
    # Copy the item
    # -------------
    items.copy_item_to_destination_iz(i)

