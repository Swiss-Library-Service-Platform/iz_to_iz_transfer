from typing import Optional
from almapiwrapper.inventory import IzBib, NzBib, Holding, Item, Collection

from utils import xlstools
from utils.processmonitoring import ProcessMonitor
from copy import deepcopy

import logging

config = xlstools.get_config()


def get_source_holding(i: int) -> Optional[Item]:
    """
    Retrieves the source holding based on the index provided in the DataFrame.

    Parameters
    ----------
    i : int
        The index of the row to process in the DataFrame.

    Returns
    -------
    Item, Optional
        The source item object.
    """
    process_monitor = ProcessMonitor()

    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']

    holding_s = Holding(mms_id_s, holding_id_s, zone=config['iz_s'], env=config['env'])

    _ = holding_s.data

    if holding_s.error:
        logging.error(f"{repr(holding_s)}: {holding_s.error_msg}")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == mms_id_s, 'Error'] = 'Source Holding not found'
        process_monitor.save()
        return None

    # From the source holding, we get the library and location of the destination IZ according to the mapping
    library_d, location_d = xlstools.get_corresponding_location(holding_s.library, holding_s.location)
    if library_d is None or location_d is None:
        logging.error(f"{repr(holding_s)}: Library or location not found in destination IZ")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == mms_id_s, 'Error'] = 'Library or location not found in destination IZ'
        return None

    return holding_s


def copy_holding_data(i: int, holding_s: Holding) -> Optional[Holding]:
    """
    Copies holding data from the source IZ to the destination IZ.

    Parameters
    ----------
    i : int
        The index of the row in the process monitor DataFrame.
    holding_s : Holding
        The source holding object containing the data to be copied.

    Returns
    -------
    Holding, Optional
        The holding object in the destination IZ, or None if an error occurs.
    """

    # Load configuration and initialize process monitor
    process_monitor = ProcessMonitor()
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']

    # From the source holding, we get the library and location of the destination IZ according to the mapping
    library_d, location_d = xlstools.get_corresponding_location(holding_s.library, holding_s.location)
    if library_d is None or location_d is None:
        logging.error(f"{repr(holding_s)}: Library or location not found in destination IZ")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == mms_id_s, 'Error'] = 'Library or location not found in destination IZ'
        return None

    # Holding should be already existing in the destination IZ, so we fetch it
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
        callnumber_code = callnumber.get('code')
        if holding_d.data.find(f'.//datafield[@tag="852"]/subfield[@code="{callnumber_code}"]') is not None:
            holding_d.data.find(f'.//datafield[@tag="852"]/subfield[@code="{callnumber_code}"]').text = callnumber.text
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

    return holding_d


def copy_holding_to_destination_iz(i: int, bib_d: IzBib) -> Optional[Holding]:
    """
    Copies holding data from the source IZ to the destination IZ.

    Parameters
    ----------
    i : int
        The index of the row in the process monitor DataFrame.
    bib_d : IzBib
        The destination IZ Bib object. If None, it will be created based on the corresponding MMS ID.

    Returns
    -------
    Holding
        The holding object in the destination IZ.
    """
    process_monitor = ProcessMonitor()

    # Retrieve the source holding and its details
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']

    holding_s = Holding(mms_id_s, holding_id_s, zone=config['iz_s'], env=config['env'])

    if holding_s.error:
        logging.error(f"{repr(holding_s)}: {holding_s.error_msg}")
        process_monitor.df.loc[process_monitor.df['Holding_id_s'] == holding_id_s, 'Error'] = 'Source Holding not found'
        process_monitor.save()
        return None

    # We need destination b to check if a corresponding holding already exists
    if bib_d is None:
        bib_d = IzBib(mms_id_d, zone=config['iz_d'], env=config['env'])
        _ = bib_d.data  # Ensure the bib data is loaded

    if bib_d.error:
        logging.error(f"{repr(bib_d)}: {bib_d.error_msg}")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == mms_id_s, 'Error'] = 'Destination Bib not found'
        process_monitor.save()
        return None

    # From the source holding, we get the library and location of the destination IZ according to the mapping
    library_d, location_d = xlstools.get_corresponding_location(holding_s.library, holding_s.location)
    if library_d is None or location_d is None:
        logging.error(f"{repr(holding_s)}: Library or location not found in destination IZ")
        process_monitor.df.loc[process_monitor.df['Holding_id_s'] == holding_id_s, 'Error'] = 'Library or location not found in destination IZ'
        process_monitor.save()
        return None

    # Check if the callnumber exists already in destination holding
    callnumber_s = holding_s.callnumber

    # Suppress empty chars from call numbers
    if callnumber_s is not None:
        callnumber_s = callnumber_s.strip()


    holding_d = None

    # Check if exists a destination holding with the same callnumber
    for holding in bib_d.get_holdings():
        callnumber_d = holding.callnumber

        # Suppress empty chars from call numbers
        if callnumber_d is not None:
            callnumber_d = callnumber_d.strip()

        if callnumber_d == callnumber_s and holding.library == library_d and holding.location == location_d and holding.error is False:
            logging.warning(f'{repr(holding_s)}: holding found with same callnumber "{callnumber_s}" and corresponding library "{library_d}" and location "{location_d}" in destination IZ.')
            holding_d = holding
            break


    # No corresponding holding found, we create a new one
    if holding_d is None:
        holding_data = deepcopy(holding_s.data)

        # Update the holding data with the destination library and location
        subfield_b = holding_data.find('.//datafield[@tag="852"]/subfield[@code="b"]')
        if subfield_b is not None:
            subfield_b.text = library_d
        else:
            logging.warning(f"{repr(holding_s)}: Subfield 'b' not found in 852 for destination holding.")

        subfield_c = holding_data.find('.//datafield[@tag="852"]/subfield[@code="c"]')
        if subfield_c is not None:
            subfield_c.text = location_d
        else:
            logging.warning(f"{repr(holding_s)}: Subfield 'c' not found in 852 for destination holding.")

        holding_d = Holding(mms_id=mms_id_d, zone=config['iz_d'], env=config['env'], data=holding_data, create_holding=True)

        if holding_d.error:
            logging.error(f"{repr(holding_d)}: {holding_d.error_msg}")
            process_monitor.df.loc[process_monitor.df['Holding_id_s'] == holding_id_s, 'Error'] = 'Destination Holding not created'
            process_monitor.save()
            return None

    return holding_d
