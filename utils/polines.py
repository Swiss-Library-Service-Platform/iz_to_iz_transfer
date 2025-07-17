import logging
from copy import deepcopy

from utils.processmonitoring import ProcessMonitor
from utils import bibs, holdings, items, xlstools

import pandas as pd

from almapiwrapper.acquisitions import POLine, Vendor, Invoice, fetch_invoices


def copy_poline(i: int) -> None:
    """
    Copies a PoLine from the source to the destination based on the provided index and configuration.

    Parameters
    ----------
    i : int
        The index of the row to process.

    Returns
    -------
    None
    """
    config = xlstools.get_config()
    process_monitor = ProcessMonitor()
    pol_number_s = process_monitor.df.at[i, 'PoLine_s']
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']

    # -----------------------
    # Fetch the source PoLine
    # -----------------------

    # Fetch the source PoLine
    pol_s = POLine(pol_number_s, config['iz_s'], config['env'])
    pol_data = deepcopy(pol_s.data)

    # Check if the source PoLine was fetched successfully
    if pol_s.error:
        logging.error(f"{repr(pol_s)}: {pol_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'POLine not found'
        process_monitor.save()
        return None

    # Check if the source PoLine has the expected MMS ID according to the Excel sheet
    if pol_s.data['resource_metadata']['mms_id']['value'] != mms_id_s:
        logging.error(f"{repr(pol_number_s)}: {pol_s.data['resource_metadata']['mms_id']['value']}"
                      f"does not match the expected provided MMS ID {mms_id_s}")
        process_monitor.df.at[i, 'Error'] = 'MMS ID mismatch'
        process_monitor.save()
        return None

    # get the purchase type of the PoLine
    pol_purchase_type = pol_s.data['type']['value']

    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)
    if mms_id_d is None:
        mms_id_d = bibs.copy_bib_from_nz_to_dest_iz(mms_id_s)

    # If the destination MMS ID is None, we cannot proceed
    if mms_id_d is None:
        return None

    # ---------------------------
    # Prepare the new PoLine data
    # ---------------------------

    # Update locations
    locations = []
    for loc in pol_data['location']:
        library_s = loc['library']['value']
        location_s = loc['shelving_location']
        library_d, location_d = xlstools.get_corresponding_location(library_s, location_s)
        if library_d is None or location_d is None:
            logging.error(f"{repr(pol_s)}: Location not found in mapping for library {library_s} and location {location_s}.")
            process_monitor.df.at[i, 'Error'] = 'Mapping: location not found'
            process_monitor.save()
            return None

        locations.append({
            "quantity": 1,
            "library": {"value": library_d},
            "shelving_location": location_d
        })

    pol_data['location'] = locations

    # Remove all alerts
    pol_data['alert'] = []

    # Update resource metadata with the new MMS ID of the other IZ
    pol_data['resource_metadata']['mms_id']['value'] = mms_id_d

    # Update owner
    library_d = xlstools.get_corresponding_library(pol_data['owner']['value'])
    if library_d is None:
        logging.error(f"{repr(pol_s)}: Library not found in mapping for library {pol_data['owner']['value']}.")
        process_monitor.df.at[i, 'Error'] = 'Mapping: library not found'
        process_monitor.save()
        return None
    pol_data['owner']['value'] = library_d

    # Update fund distribution
    for fund in pol_data['fund_distribution']:
        fund_code_d = xlstools.get_corresponding_fund(fund['fund_code']['value'])

        if fund_code_d is None:
            logging.error(f"{repr(pol_s)}: Fund code not found in mapping for fund {fund['fund_code']['value']}.")
            process_monitor.df.at[i, 'Error'] = 'Mapping: fund code not found'
            process_monitor.save()
            return None

        fund['fund_code']['value'] = fund_code_d
        fund['amount']['currency']['value'] = 'CHF'


    # Update vendor code and vendor account
    vendor_code_d, vendor_account_d = xlstools.get_corresponding_vendor(
        pol_data['vendor']['value'], pol_data['vendor_account']
    )
    if vendor_code_d is None or vendor_account_d is None:
        logging.error(f"{repr(pol_s)}: Vendor or vendor account not found in mapping for vendor {pol_data['vendor']['value']} "
                      f"and account {pol_data['vendor_account']}.")
        process_monitor.df.at[i, 'Error'] = 'Mapping: vendor or vendor account not found'
        process_monitor.save()
        return None
    pol_data['vendor']['value'] = vendor_code_d
    pol_data['vendor_account'] = vendor_account_d

    # PO Line number will change in the new IZ, we keep the old one in the additional_order_reference field
    pol_data['additional_order_reference'] = pol_s.pol_number

    # ---------------------
    # Create the new PoLine
    # ---------------------

    pol_d = POLine(data=pol_data, zone=config['iz_d'], env=config['env']).create()

    # Check if the PoLine was created successfully
    if pol_d.error:
        logging.error(f"{repr(pol_d)}: {pol_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'POLine not created'
        process_monitor.save()
        return None

    # Update the process monitor with the new PoLine number
    process_monitor.set_corresponding_poline(pol_number_s, pol_d.pol_number, pol_purchase_type)
    process_monitor.save()


def process(i: int) -> None:
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
        copy_poline(i)

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
    items.copy_item_to_the_destination_iz(i, poline=True)
