from utils import polines, bibs, holdings, items, xlstools
from utils.processmonitoring import ProcessMonitor
import logging
import pandas as pd
from almapiwrapper.inventory import IzBib, Holding, Item
from almapiwrapper.acquisitions import POLine

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
    holding_s = None
    pol_d = None
    holding_d = None

    # Check if the row is already copied
    if process_monitor.df.at[i, 'Copied']:
        # If the row is already copied, we skip it
        return None

    # Get the source PoLine number, MMS ID, Holding ID, and Item ID
    pol_number_s = process_monitor.df.at[i, 'PoLine_s']
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']
    item_id_s = process_monitor.df.at[i, 'Item_id_s']
    item_id_d = process_monitor.get_corresponding_item_id(item_id_s)

    pol_number_d, pol_purchase_type = process_monitor.get_corresponding_poline(pol_number_s)
    if pol_number_d is None:
        pol_d = polines.copy_poline(i)

    # The corresponding MMS ID in the destination IZ should exist now
    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)
    pol_number_d, pol_purchase_type = process_monitor.get_corresponding_poline(pol_number_s)

    # If the destination PoLine number or MMS ID is None, we cannot proceed
    if mms_id_d is None or pol_number_d is None:
        return None

    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)

    # If the destination holding ID is None, we need to copy the holding data
    if holding_id_d is None:
        # Copy data from the source holding to the destination IZ
        holding_s = holdings.get_source_holding(i)
        if holding_s is None:
            # If the source holding could not be retrieved, we skip the row
            return None
        holding_d = holdings.copy_holding_data(i, holding_s)

        if holding_d is None or holding_d.error:
            # If the destination holding could not be created, we skip the row
            return None

    # if the destination holding is available, we retrieve the holding using the mms ID and holding ID
    elif item_id_d is None and pd.notnull(item_id_s):
        holding_d = Holding(mms_id_d, holding_id_d, zone=config['iz_d'], env=config['env'])
        _ = holding_d.data
        if holding_d.error:
            logging.error(f"{repr(holding_d)}: {holding_d.error_msg}")
            process_monitor.df.at[i, 'Error'] = 'Destination Holding not found'
            process_monitor.save()
            return None

    # Check if the holding could be retrieved
    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)
    if holding_id_d is None:
        return None

    if pd.isnull(item_id_s):
        # If the item ID is NaN, we skip the item processing
        logging.warning(f"Item ID is NaN for row {i}, skipping item processing.")
        process_monitor.df.at[i, 'Copied'] = True
        process_monitor.save()
        return None

    # We check the purchase type of the PoLine

    if pol_purchase_type.endswith('_CO'):
        # In case of continuous orders, we copy the item to the destination IZ
        # The PoLine is linked to the holding and the item don't exist in the destination IZ
        items.copy_item_to_destination_iz(i, poline=True)

    elif pol_purchase_type.endswith('_OT'):
        # In case of one-time orders, the item is linked to the PoLine and
        # the item should already exist in the destination IZ
        # We need to receive it if already received in the source IZ

        if item_id_d is None:

            if holding_s is None:
                holding_s = holdings.get_source_holding(i)
            if holding_s is None:
                # If the source holding could not be retrieved, we skip the row
                return None

            if pol_d is None:
                pol_d = POLine(pol_number_d, zone=config['iz_d'], env=config['env'])
                _ = pol_d.data  # Ensure the PoLine data is loaded
            if pol_d.error:
                logging.error(f"{repr(pol_d)}: {pol_d.error_msg}")
                process_monitor.df.at[i, 'Error'] = 'Destination PoLine not found'
                process_monitor.save()
                return None

            item_d = items.handle_one_time_pol_items(i, holding_s, holding_d, pol_d)
            if item_d is None or item_d.error:
                return None

        if config['make_reception'] and process_monitor.df.at[i, 'Received']:
            pol_d = items.make_reception(i)
            if pol_d is None or pol_d.error:
                return None
    else:
        # If the purchase type is not continuous or one-time, we skip the item processing
        logging.warning(f"Unknown purchase type '{pol_purchase_type}' for row {i}, skipping item processing.")
        process_monitor.df.at[i, 'Error'] = 'Unknown purchase type'
        process_monitor.save()
        return None

    return None


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

    item_s = items.get_source_item_using_barcode(i)
    if item_s is None:
        # If the source item could not be retrieved, we skip the row
        return None

    item_id_s = item_s.get_item_id()
    holding_id_s = item_s.get_holding_id()
    iz_mms_id_s = item_s.get_mms_id()

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
        _ = holdings.copy_holding_to_destination_iz(i, bib_d)

    # -------------
    # Copy the item
    # -------------
    _ = items.copy_item_to_destination_iz(i)

    return None


def holding(i: int) -> None:
    """
    Processes all items in the process monitor DataFrame.

    This function iterates through each row in the process monitor DataFrame,
    checks if the item has already been copied, and if not, processes it.
    """
    process_monitor = ProcessMonitor()

    if process_monitor.df.at[i, 'Copied']:
        # If the row is already copied, we skip it
        return None

    # --------
    # Copy bib
    # --------
    iz_mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']
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
        _ = holdings.copy_holding_to_destination_iz(i, bib_d)

    return None

def bib(i: int) -> None:
    """
    Processes a single row in the process monitor DataFrame for bib records.

    Parameters
    ----------
    i : int
        The index of the row to process.

    Returns
    -------
    None
    """
    process_monitor = ProcessMonitor()

    if process_monitor.df.at[i, 'Copied']:
        # If the row is already copied, we skip it
        return None

    iz_mms_id_s = process_monitor.df.at[i, 'MMS_id_s']

    # Copy the bib record from the source IZ to the destination IZ
    bib_d = bibs.copy_bib_from_nz_to_dest_iz(iz_mms_id_s)

    if bib_d is None:
        # If the destination bib could not be created, we skip the row
        return None

    # Mark the row as copied
    process_monitor.df.at[i, 'Copied'] = True
    process_monitor.save()

    return None
