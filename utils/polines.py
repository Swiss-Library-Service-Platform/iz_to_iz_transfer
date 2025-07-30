import logging
from copy import deepcopy

from utils.processmonitoring import ProcessMonitor
from utils import bibs, holdings, items, xlstools

from almapiwrapper.acquisitions import POLine, Vendor, Invoice, fetch_invoices
from almapiwrapper.users import User

from typing import Optional

config = xlstools.get_config()


def copy_poline(i: int) -> Optional[POLine]:
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
        bib_d = bibs.copy_bib_from_nz_to_dest_iz(mms_id_s)
        mms_id_d = bib_d.get_mms_id() if bib_d else None

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
        quantity = 1 if pol_purchase_type.endswith('_CO') else loc['quantity']
        library_d, location_d = xlstools.get_corresponding_location(library_s, location_s)
        if library_d is None or location_d is None:
            logging.error(f"{repr(pol_s)}: Location not found in mapping for library {library_s} and location {location_s}.")
            process_monitor.df.at[i, 'Error'] = 'Mapping: location not found'
            process_monitor.save()
            return None

        locations.append({
            "quantity": quantity,
            "library": {"value": library_d},
            "shelving_location": location_d
        })
    pol_data['location'] = locations

    if pol_purchase_type.endswith('_OT'):
        pol_data['acquisition_method']['value'] = 'VENDOR_SYSTEM'

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
        # fund['amount']['currency']['value'] = 'CHF'


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
    pol_data['po_number'] = ''

    # Check interested users
    pol_data = handle_interested_users(pol_data)
    if pol_data is None:
        process_monitor.df.at[i, 'Error'] = 'Interested user not found'
        process_monitor.save()
        return None


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

    return pol_d


def handle_interested_users(pol_data: dict) -> Optional[dict]:
    """
    Handle interested users for the PoLine.

    Parameters
    ----------
    pol_data : dict
        The PoLine data dictionary.
    """
    interested_users = []
    if ('interested_user' in pol_data and
        pol_data['interested_user'] and
        'interested_user' not in config['polines_fields']['to_delete']):

        for interested_user in pol_data['interested_user']:
            primary_id = interested_user['primary_id']
            if primary_id not in config['interested_users']:
                user = User(primary_id, zone=config['iz_d'], env=config['env'])
                _ = user.data
                if user.error:
                    if 'interested_user' in config['polines_fields']['to_delete_if_error']:
                        logging.warning(f"{repr(user)}: interested user not found, skipping: {user.error_msg}")
                        continue
                    else:
                        logging.error(f"{repr(user)}: interested user not found")
                        return None
            config['interested_users'].append(primary_id)
            interested_users.append(deepcopy(interested_user))

    pol_data['interested_user'] = interested_users
    return pol_data
