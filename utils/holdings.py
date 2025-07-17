from typing import Optional
from almapiwrapper.inventory import IzBib, NzBib, Holding, Item, Collection

from utils import xlstools
from utils.processmonitoring import ProcessMonitor
from copy import deepcopy

import logging

def copy_holding_data(mms_id_s: str, holding_id_s: str, mms_id_d: str) -> None:
    """
    Copies holding data from the source IZ to the destination IZ.

    Parameters
    ----------
    mms_id_s : str
        The MMS ID of the bib in the source IZ.
    holding_id_s : str
        The holding ID in the source IZ.
    mms_id_d : str
        The MMS ID of the bib in the destination IZ.

    Returns
    -------
    None
    """

    # Load configuration and initialize process monitor
    config = xlstools.get_config()
    process_monitor = ProcessMonitor()

    # Fetch the source holding
    holding_s = Holding(mms_id_s, holding_id_s, zone=config['iz_s'], env=config['env'])
    hol_data = deepcopy(holding_s.data)

    if holding_s.error:
        logging.error(f"{repr(holding_s)}: {holding_s.error_msg}")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == mms_id_s, 'Error'] = 'Source Holding not found'
        return None

    # From the source holding, we get the library and location of the destination IZ according to the mapping
    library_d, location_d = xlstools.get_corresponding_location(holding_s.library, holding_s.location)
    if library_d is None or location_d is None:
        logging.error(f"{repr(holding_s)}: Library or location not found in destination IZ")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == mms_id_s, 'Error'] = 'Library or location not found in destination IZ'
        return None

    # Create a new holding in the destination IZ
    bib_d = IzBib(mms_id_d, zone=config['iz_d'], env=config['env'])
    hols_d = [hol for hol in bib_d.get_holdings() if hol.library == library_d and hol.location == location_d]

    # If no matching holdings are found, the PoLine might not have been created yet
    if len(hols_d) == 0:
        logging.error(f"{repr(holding_s)}: No matching holdings found in destination IZ for {mms_id_d}, library {library_d} and location {location_d}.")
        process_monitor.df.loc[process_monitor.df['Holding_id_s'] == holding_id_s, 'Error'] = 'No matching holdings found in destination IZ'
        return None

    # If there are multiple holdings, we take the last one, it is probably the last created with the PoLine
    if len(hols_d) > 1:
        logging.warning(f"{repr(holding_s)}: Multiple matching holdings found in destination IZ for {mms_id_d}, library {library_d} and location {location_d}. Using the last one.")
        holding_d = hols_d[-1]
    else:
        holding_d = hols_d[0]

    if holding_d.error:
        logging.error(f"{repr(holding_d)}: {holding_d.error_msg}")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == mms_id_s, 'Error'] = 'Destination Holding not retrieved'
        return None

    # Update the holding data with data of the source holding
    hol_fields = [f for f in holding_s.data.findall('.//datafield') if
                  f.get('tag') != '852' and f.get('ind1') in ' 0123456789' and f.get('ind2') in ' 0123456789']

    # Copy fields like 853 or 866
    for f in hol_fields:
        holding_d.data.find('record').append(deepcopy(f))

    # Copy the call number
    # Get callnumber from 852$$h or 852$$j
    callnumber = holding_s.data.find('.//datafield[@tag="852"]/subfield[@code="h"]')
    if callnumber is None:
        callnumber = holding_s.data.find('.//datafield[@tag="852"]/subfield[@code="j"]')

    if callnumber is None:
        logging.warning(f"{repr(holding_s)}: No call number found in source holding.")
    else:
        holding_d.data.find('.//datafield[@tag="852"]').append(callnumber)

    # Remove $$t subfield of 852
    for f in holding_d.data.findall('.//datafield[@tag="852"]/subfield[@code="t"]'):
        f.getparent().remove(f)

    holding_d.update()

    # Check if the holding was updated successfully
    if holding_d.error:
        logging.error(f"{repr(holding_d)}: {holding_d.error_msg}")
        process_monitor.df.loc[process_monitor.df['Holding_id_s'] == holding_id_s, 'Error'] = 'Destination Holding not updated'
        return None

    # Update the process monitor with the new holding ID
    process_monitor.set_corresponding_holding_id(holding_id_s, holding_d.holding_id)
    process_monitor.save()
