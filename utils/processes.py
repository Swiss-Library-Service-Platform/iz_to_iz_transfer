import logging
from http.cookiejar import user_domain_match

import pandas as pd
from almapiwrapper.acquisitions import POLine
from almapiwrapper.inventory import IzBib, Holding, Item, Collection
from almapiwrapper.users import User, Request

from utils import polines, bibs, holdings, items, xlstools, loans, requests
from utils.processmonitoring import ProcessMonitor

config = xlstools.get_config()


def poline(i: int) -> None:
    """
    Processes a single row in the process monitor DataFrame.

    Parameters
    ----------
    i : int
        The index of the row to process.
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

    # -----------
    # Copy PoLine
    # ------------
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
        holding_id_d = holding_d.get_holding_id() if holding_d else None

        if holding_id_d is None:
            # If the destination holding could not be created, we skip the row
            return None

        # Update the process monitor with the new holding ID
        process_monitor.set_corresponding_holding_id(holding_id_s, holding_id_d)
        process_monitor.save()

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

    # ------------------------------
    # Copy items of continuous order
    # ------------------------------
    if pol_purchase_type.endswith('_CO'):
        # In case of continuous orders, we copy the item to the destination IZ
        # The PoLine is linked to the holding and the item don't exist in the destination IZ
        items.copy_item_to_destination_iz(i, poline=True)

    # ------------------------------
    # Update items of one time order
    # ------------------------------
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

            item_d = items.handle_one_time_pol_items(i, holding_s, holding_d)
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

    Parameters
    ----------
    i : int
        The index of the row to process.
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
        mms_id_d = bib_d.get_mms_id() if bib_d else None

        if mms_id_d is None:
            # If the destination bib could not be created, we skip the row
            return None

    process_monitor.set_corresponding_mms_id(iz_mms_id_s, mms_id_d)
    process_monitor.save()

    # ------------
    # Copy holding
    # ------------
    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)
    if holding_id_d is None:
        # Copy the holding data from the source to the destination IZ
        holding_d = holdings.copy_holding_to_destination_iz(i, bib_d)
        holding_id_d = holding_d.get_holding_id() if holding_d else None

        if holding_id_d is None:
            # If the destination holding could not be created, we skip the row
            return None

    process_monitor.set_corresponding_holding_id(holding_id_s, holding_id_d)
    process_monitor.save()

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
        mms_id_d = bib_d.get_mms_id() if bib_d else None
        if mms_id_d is None:
            # If the destination bib could not be created, we skip the row
            return None

    # we need to save the corresponding MMS ID
    process_monitor.set_corresponding_mms_id(iz_mms_id_s, mms_id_d)
    process_monitor.save()

    # ------------
    # Copy holding
    # ------------
    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)
    if holding_id_d is None:
        # Copy the holding data from the source to the destination IZ
        holding_d = holdings.copy_holding_to_destination_iz(i, bib_d)
        holding_id_d = holding_d.get_holding_id() if holding_d else None
        if holding_id_d is None:
            return None

    process_monitor.set_corresponding_holding_id(holding_id_s, holding_id_d)
    process_monitor.df.at[i, 'Copied'] = True
    process_monitor.save()

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
    mms_id_d = bib_d.get_mms_id() if bib_d else None

    if mms_id_d is None:
        # If the destination bib could not be created, we skip the row
        return None

    # Mark the row as copied
    process_monitor.set_corresponding_mms_id(iz_mms_id_s, mms_id_d)
    process_monitor.df.at[i, 'Copied'] = True
    process_monitor.save()

    return None


def collection(i: int) -> None:
    """
    Processes a single row in the process monitor DataFrame for collection records.

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

    # Get source collection information
    collection_id_s = process_monitor.df.at[i, 'Collection_id_s']
    col_s = Collection(collection_id_s, zone=config['iz_s'], env=config['env'])
    bibs_s = col_s.bibs

    if col_s.error:
        logging.error(f"{repr(col_s)}: {col_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source Collection not found'
        process_monitor.save()
        return None

    # Get destination collection information
    collection_id_d = process_monitor.df.at[i, 'Collection_id_d']
    col_d = Collection(collection_id_d, zone=config['iz_d'], env=config['env'])
    bibs_d = col_d.bibs

    if col_d.error:
        logging.error(f"{repr(col_d)}: {col_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Destination Collection not found'
        process_monitor.save()
        return None

    mms_id_col_d = [bib.get_mms_id() for bib in bibs_d]

    for bib_s in bibs_s:
        # Copy each bib from the source collection to the destination collection
        bib_d = bibs.get_corresponding_bib_from_col(bib_s, i)
        mms_id_d = bib_d.get_mms_id() if bib_d else None

        if bib_d is None or bib_d.error:
            continue

        if mms_id_d in mms_id_col_d:
            logging.warning(f"{col_d}: {mms_id_d} already in the collection")
            continue

        # Add the bib to the destination collection
        mms_id_col_d.append(mms_id_d)
        col_d.add_bib(bib_d)

    # Mark the row as copied
    if len(bibs_s) == len(mms_id_col_d):
        process_monitor.df.at[i, 'Copied'] = True
        process_monitor.save()
        logging.info(f'{repr(col_s)}: collection completed with {len(mms_id_col_d)} bibs')
    else:
        logging.error(f'{repr(col_s)}: collection not completed, {len(mms_id_col_d)} bibs copied out of {len(bibs_s)}')
        process_monitor.df.at[i, 'Error'] = 'Collection not completed'
        process_monitor.save()

    return None


def loan(i: int) -> None:
    """
    Processes a single row in the process monitor DataFrame for loan records.

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
    print(config['make_loans'])
    # -------------------------
    # Create loan using item id
    # -------------------------
    if (config['make_loans'] and
            pd.isnull(process_monitor.df.at[i, 'Barcode_d']) and
            (pd.notnull(process_monitor.df.at[i, 'Item_id_d']) or
             pd.notnull(process_monitor.df.at[i, 'Barcode_s'])) and not (
                    pd.isnull(process_monitor.df.at[i, 'Item_id_d']) and
                    pd.isnull(process_monitor.df.at[i, 'Barcode_s'])
            )):

        # Create a loan for the item in the destination IZ
        loan_d = loans.create_loan(i)

        if loan_d is None or loan_d.error:
            if loan_d is not None:
                logging.error(f"{repr(loan_d)}: {loan_d.error_msg}")
            process_monitor.df.at[i, 'Error'] = 'Destination item not loaned'
            process_monitor.save()
            return None
        else:
            # If the loan was successful, we update the DataFrame
            if pd.isnull(process_monitor.df.at[i, 'Barcode_d']):
                process_monitor.df.at[i, 'Barcode_d'] = loan_d.data['item_barcode']
            if pd.isnull(process_monitor.df.at[i, 'Item_id_d']):
                process_monitor.df.at[i, 'Item_id_d'] = loan_d.data['item_id']
                process_monitor.df.at[i, 'Holding_id_d'] = loan_d.data['holding_id']
                process_monitor.df.at[i, 'MMS_id_d'] = loan_d.data['mms_id']
            process_monitor.save()

    # -----------
    # Make return
    # -----------
    if (
            config['make_returns']
            and (
            pd.isnull(process_monitor.df.at[i, 'Barcode_s'])
            or pd.isnull(process_monitor.df.at[i, 'Item_id_s']))
            and not (
            pd.isnull(process_monitor.df.at[i, 'Barcode_s'])
            and pd.isnull(process_monitor.df.at[i, 'Item_id_s']))
    ):

        item_s = loans.make_return(i)

        if item_s is None or item_s.error:
            if item_s is not None:
                logging.error(f"{repr(item_s)}: {item_s.error_msg}")
            process_monitor.df.at[i, 'Error'] = 'Source item not returned'
            process_monitor.save()
            return None
        else:
            # If the return was successful, we update the DataFrame
            if pd.isnull(process_monitor.df.at[i, 'Barcode_s']):
                process_monitor.df.at[i, 'Barcode_s'] = item_s.barcode
            if pd.isnull(process_monitor.df.at[i, 'Item_id_s']):
                process_monitor.df.at[i, 'Item_id_s'] = item_s.get_item_id()
                process_monitor.df.at[i, 'Holding_id_s'] = item_s.get_holding_id()
                process_monitor.df.at[i, 'MMS_id_s'] = item_s.get_mms_id()
            process_monitor.save()

    # If we reach this point, we have successfully processed the loan or return
    process_monitor.df.at[i, 'Copied'] = True
    process_monitor.save()

    return None


def request(i: int) -> None:
    """
    Processes a single row in the process monitor DataFrame for request records.

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

    # Get the source request information
    primary_id = process_monitor.df.at[i, 'Primary_id']
    request_id_s = process_monitor.df.at[i, 'Request_id_s']
    request_s = Request(request_id=request_id_s, user_id=primary_id, zone=config['iz_s'], env=config['env'])
    _ = request_s.data  # Ensure the request data is loaded

    if request_s.error:
        logging.error(f"{repr(request_s)}: {request_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source Request not found'
        process_monitor.save()
        return None

    # ------------------------------------
    # Create request in the destination IZ
    # ------------------------------------
    if pd.isnull(process_monitor.df.at[i, 'Request_id_d']):
        request_d = requests.create_request(i, request_s)

        if request_d is None or request_s.error:
            if request_d is not None:
                logging.error(f"{repr(request_d)}: {request_d.error_msg}")
            else:
                logging.error(f"Request with ID {request_id_s} could not be created.")
            process_monitor.df.at[i, 'Error'] = 'Source Request not created'
            process_monitor.save()
            return None

        process_monitor.df.at[i, 'Request_id_d'] = request_d.request_id
        process_monitor.save()

    # ---------------------
    # Cancel source request
    # ---------------------
    if pd.notnull(process_monitor.df.at[i, 'Request_id_d']):
        request_s.save()
        request_s.cancel(reason=config['cancel_reason'], note=config['cancel_note'], notify_user=True if config['cancel_note'] else False)

        if request_s.error:
            logging.error(f"{repr(request_s)}: {request_s.error_msg}")
            process_monitor.df.at[i, 'Error'] = 'Source Request not cancelled'
            process_monitor.save()
            return None

        # Mark the row as copied
        process_monitor.df.at[i, 'Copied'] = True
        process_monitor.save()

    return None
