from typing import Optional
from almapiwrapper.inventory import IzBib, NzBib, Holding, Item, Collection
from almapiwrapper.acquisitions import POLine
import time

from utils import xlstools
from utils.processmonitoring import ProcessMonitor
from copy import deepcopy
from lxml import etree
import pandas as pd

import logging

config = xlstools.get_config()


def get_source_item_using_barcode(i: int) -> Optional[Item]:
    """
    Retrieves the source item based on the index provided in the DataFrame.

    Parameters
    ----------
    i : int
        The index of the row to process in the DataFrame.

    Returns
    -------
    Item, optional
        The source item object, or None if an error occurs.
    """
    process_monitor = ProcessMonitor()

    barcode = process_monitor.df.at[i, 'Barcode']

    item_s = Item(barcode=barcode, zone=config['iz_s'], env=config['env'])

    _ = item_s.data
    if item_s.error:
        # check if item already exists in the destination
        item_d_test = Item(barcode=barcode, zone=config['iz_d'], env=config['env'])
        item_s_test = Item(barcode='OLD_' + barcode, zone=config['iz_s'], env=config['env'])

        # Test if the item exists in the destination IZ and if the source record's barcode has already been updated
        if item_d_test.error is False and item_s_test.error is False:
            error_label = 'Item exists in the dest IZ and barcode of source record updated'

        elif item_d_test.error is False:
            error_label = 'Barcode already exists in the destination IZ'

        else:
            error_label = 'Source Item not found'

        logging.error(f"{repr(item_s)}: {item_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = error_label
        process_monitor.save()
        return None

    return item_s


def copy_item_to_destination_iz(i, poline: bool = False) -> Optional[Item]:
    """
    Copies an item from the source IZ to the destination IZ based on the provided index and configuration.
    This function retrieves the source item, updates its library and location according to the destination IZ,
    and creates a new item in the destination IZ. If the item is associated with a PoLine, it updates the PoLine
    information as well.

    Parameters
    ----------
    i : int
        The index of the row to process in the DataFrame.
    poline : bool, optional
        If True, the function will also handle the PoLine information for the item.

    Returns
    -------
    Item, optional
        The newly created item in the destination IZ, or None if an error occurs.
    """
    # Load configuration and initialize process monitor
    process_monitor = ProcessMonitor()

    # Get the source and destination MMS IDs
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)

    # Get the source holding ID
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']
    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)

    # Fetch the source item
    item_id_s = process_monitor.df.at[i, 'Item_id_s']

    pol_number_d = None

    # fetch the source and destination poline
    if poline:
        pol_number_s = process_monitor.df.at[i, 'PoLine_s']
        pol_number_d, _ = process_monitor.get_corresponding_poline(pol_number_s)

    item_s = Item(mms_id_s, holding_id_s, item_id_s, zone=config['iz_s'], env=config['env'])
    library_s = item_s.library
    location_s = item_s.location

    if item_s.error:
        logging.error(f"{repr(item_s)}: {item_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source Item not found'
        process_monitor.save()
        return None

    # Update the item data with the destination library and location
    item_data = deepcopy(item_s.data)

    library_d, location_d = xlstools.get_corresponding_location(library_s, location_s)
    if library_d is None or location_d is None:
        logging.error(f"{repr(item_s)}: Library or location not found in destination IZ")
        process_monitor.df.at[i, 'Error'] = 'Library or location not found in destination IZ'
        process_monitor.save()
        return None

    item_data.find('.//item_data/library').text = library_d
    item_data.find('.//item_data/location').text = location_d

    if poline:
        item_data.find('.//item_data/po_line').text = pol_number_d

        arrival_date = item_data.find('.//arrival_date')
        expected_arrival_date = item_data.find('.//expected_arrival_date')
        process_type = item_data.find('.//process_type')

        # Determine if the item is received based on the arrival date and expected arrival date
        if arrival_date is None and expected_arrival_date is not None and process_type.text == 'ACQ':
            received = False
        else:
            received = True

        process_monitor.df.at[i, 'Received'] = received
        process_monitor.save()

    # Clean the item fields before creating the item in the destination IZ
    item_data = clean_item_fields(item_data, rec_loc='dest', retry=False)
    item_d = Item(mms_id_d, holding_id_d, zone=config['iz_d'], env=config['env'], data=item_data, create_item=True)

    # Retry creating the item if it failed
    if item_d.error:
        # Clean the item fields before creating the item in the destination IZ
        item_data = clean_item_fields(item_data, rec_loc='dest', retry=True)
        item_d = Item(mms_id_d, holding_id_d, zone=config['iz_d'], env=config['env'], data=item_data, create_item=True)

    # Check if the item was created successfully, if not, log the error and update the process monitor
    if item_d.error:
        logging.error(f"{repr(item_d)}: {item_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Destination Item not created'
        process_monitor.save()
        return None

    process_monitor.set_corresponding_item_id(item_s.item_id, item_d.item_id)
    process_monitor.df.at[i, 'Copied'] = True
    error_msg = process_monitor.df.at[i, 'Error']
    if pd.notnull(error_msg) and len(error_msg) > 0 and ' - SOLVED' not in error_msg:
        process_monitor.df.at[i, 'Error'] += ' - SOLVED'
    process_monitor.save()

    update_source_item(item_s)

    if item_s.error:
        logging.error(f"{repr(item_s)}: failed to update barcode of source record: {item_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Failed to update source item barcode'
        return None

    return item_d


def clean_item_fields(item_data: etree.Element, rec_loc: str, retry: bool = False) -> etree.Element:
    """
    Cleans the fields of an item by removing unwanted characters and formatting.

    Parameters
    ----------
    item_data : etree.Element
        The item to clean.
    rec_loc : str
        The record location, used to determine the context of
        the item: "src" or "dest" IZ.
    retry : bool, optional
        If True, the function will clean fields that can be cleaned in case
        of error.

    Returns
    -------
    etree.Element
        The cleaned item data.
    """
    # Load configuration
    fields_to_remove = deepcopy(config['items_fields'][rec_loc]['to_delete'])

    if retry:
        fields_to_remove += config['items_fields'][rec_loc]['to_delete_if_error']

    for field in fields_to_remove:
        field_element = item_data.find(f".//{field}")

        if field_element is not None:
            if retry:
                logging.warning(f'Item {item_data.find(".//barcode").text}: delete field "{field}": "{field_element.text}"')
            field_element.getparent().remove(field_element)

    return item_data

def update_source_item(item_s: Item) -> Optional[Item]:
    """
    Updates the source item by removing the 'OLD_' prefix from the barcode.

    Parameters
    ----------
    item_s : Item
        The source item to update.

    Returns
    -------
    Item, optional
        The updated source item with the new barcode or None if an error occurs.
    """
    # Change barcode of source item
    if item_s.barcode.startswith('OLD_'):
        # Skip this step if barcode already updated
        logging.warning(f'{repr(item_s)}: barcode already updated "{item_s.barcode}"')
        return None

    item_s.save()
    item_s.barcode = 'OLD_' + item_s.barcode

    _ = clean_item_fields(item_s.data, rec_loc='src', retry=False)

    item_s = item_s.update()

    # Retry updating the source item if it failed
    if item_s.error:
        logging.error(f"{repr(item_s)}: failed to update barcode of source record: {item_s.error_msg}")

        item_s.error = False
        item_s.error_msg = None
        _ = clean_item_fields(item_s.data, rec_loc='src', retry=True)
        item_s = item_s.update()

        if item_s.error:
            logging.error(f"{repr(item_s)}: failed to update barcode of source record (retry): {item_s.error_msg}")
            return None

    return item_s


def handle_one_time_pol_items(i: int, holding_s: Holding, holding_d: Holding) -> Optional[Item]:
    """
    Retrieves the destination item from the holding based on the index provided in the DataFrame.

    Parameters
    ----------
    i : int
        The index of the row to process in the DataFrame.
    holding_s : Holding
        The source holding object from which to retrieve the item.
    holding_d : Holding
        The destination holding object where the item will be copied.

    Returns
    -------
    Item, optional
        The destination item object, or None if not found.
    """
    process_monitor = ProcessMonitor()
    item_id_s = process_monitor.df.at[i, 'Item_id_s']
    poline_id_s = process_monitor.df.at[i, 'PoLine_s']
    poline_id_d = process_monitor.get_corresponding_poline(poline_id_s)

    items_s = [item for item in holding_s.get_items()
               if item.data.find('.//item_data/po_line') is not None and
               item.data.find('.//item_data/po_line').text == poline_id_s]
    items_d = [item for item in holding_d.get_items()
               if item.data.find('.//item_data/po_line') is not None and
               item.data.find('.//item_data/po_line').text == poline_id_d]

    if len(items_d) == 0:
        # If there are no items we need to wait a few seconds and try again
        time.sleep(3)
        holding_d = Holding(holding_d.bib.mms_id, holding_d.holding_id, zone=config['iz_d'], env=config['env'])
        items_d = holding_d.get_items()

    # get item rank in source holding
    # Idea is to get the rank of the item in the source holding with the corresponding PoLine information.
    # If the PoLine has to items in the source IZ it must be 2 items in destination IZ.
    index = next((i for i, item in enumerate(items_s) if item.item_id == item_id_s), -1)

    if index == -1:
        # No matching item found in source holding
        logging.error(f"Item with ID {item_id_s} not found in source holding {holding_s.holding_id}")
        process_monitor.df.at[i, 'Error'] = 'Item not found in source holding'
        process_monitor.save()
        return None
    elif index >= len(items_d):
        # Not enough items in destination holding to match source item
        logging.error(f"Not enough items in destination holding {holding_s.holding_id} to match source item {item_id_s}")
        process_monitor.df.at[i, 'Error'] = 'Not enough items in destination holding'
        process_monitor.save()
        return None

    item_s = items_s[index]
    item_d = items_d[index]

    for field in item_s.data.find('.//item_data'):
        # Copy only specific fields from source item to destination item
        if (field.tag in ['pid', 'po_line', 'creation_date', 'modification_date', 'base_status',
                          'awaiting_reshelving', 'library', 'location', 'arrival_date'] or
                item_d.data.find(f'.//item_data/{field.tag}') is None or
                item_s.data.find(f'.//item_data/{field.tag}') is None):
            continue
        item_d.data.find(f'.//item_data/{field.tag}').text = item_s.data.find(
            f'.//item_data/{field.tag}').text
    item_d = item_d.update()

    if item_d.error:
        logging.error(f"{repr(item_d)}: {item_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Failed to update destination item'
        process_monitor.save()
        return None

    # Get the arrival date and expected arrival date from the source item
    arrival_date = item_d.data.find('.//arrival_date')
    expected_arrival_date = item_d.data.find('.//expected_arrival_date')
    process_type = item_d.data.find('.//process_type')

    # Determine if the item is received based on the arrival date and expected arrival date
    if arrival_date is None and expected_arrival_date is not None:
        received = False
    else:
        received = True

    process_monitor.df.at[i, 'Received'] = received
    process_monitor.set_corresponding_item_id(item_s.item_id, item_d.item_id)
    if not received:
        process_monitor.df.at[i, 'Copied'] = True
        error_msg = process_monitor.df.at[i, 'Error']
        if pd.notnull(error_msg) and len(error_msg) > 0 and ' - SOLVED' not in error_msg:
            process_monitor.df.at[i, 'Error'] += ' - SOLVED'
    process_monitor.save()

    update_source_item(item_s)

    if item_s.error:
        logging.error(f"{repr(item_s)}: failed to update barcode of source record: {item_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Failed to update source item barcode'
        process_monitor.save()
        return None

    return item_d


def make_reception(i: int) -> Optional[POLine]:
    """
    Makes a reception for the item based on the index provided in the DataFrame.

    Parameters
    ----------
    i : int
        The index of the row to process in the DataFrame.

    Returns
    -------
    POLine, optional
        The updated PoLine after making the reception, or None if an error occurs.
    """
    process_monitor = ProcessMonitor()
    pol_number_s = process_monitor.df.at[i, 'PoLine_s']
    item_id_s = process_monitor.df.at[i, 'Item_id_s']
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    item_id_d = process_monitor.get_corresponding_item_id(item_id_s)
    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)
    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)
    pol_number_d, pol_purchase_type = process_monitor.get_corresponding_poline(pol_number_s)

    if any([pd.isnull(identifier) for identifier in [item_id_s, holding_id_s, mms_id_s, item_id_d, holding_id_d, mms_id_d, pol_number_s, pol_number_d]]):
        logging.error('Identifier missing, impossible to make reception')
        return None

    item_s = Item(mms_id_s, holding_id_s, item_id_s, zone=config['iz_s'], env=config['env'])
    _ = item_s.data

    if item_s.error:
        logging.error(f"{repr(item_s)}: {item_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source Item not found'
        process_monitor.save()
        return None

    item_d = Item(mms_id_d, holding_id_d, item_id_d, zone=config['iz_d'], env=config['env'])
    _ = item_d.data
    if item_d.error:
        logging.error(f"{repr(item_d)}: {item_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Destination Item not found'
        process_monitor.save()
        return None

    pol_d = POLine(pol_number_d, zone=config['iz_d'], env=config['env'])
    _ = pol_d.data
    if pol_d.error:
        logging.error(f"{repr(pol_d)}: {pol_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Destination PoLine not found'
        process_monitor.save()
        return None

    # If the item is received, we set the receive date to the arrival date
    if item_s.data.find('.//item_data/arrival_date') is not None:
        arrival_date = item_s.data.find('.//item_data/arrival_date').text
        acq_department = config['acq_department']
        if acq_department:
            pol_d.receive_item(item_d,
                               receive_date=arrival_date,
                               library=item_d.library,
                               department=acq_department)
        else:
            pol_d.receive_item(item_d, receive_date=arrival_date)

    if pol_d.error:
        logging.error(f"{repr(pol_d)}: {pol_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Failed to receive item in PoLine'
        process_monitor.save()
        return None

    process_monitor.df.at[i, 'Copied'] = True
    error_msg = process_monitor.df.at[i, 'Error']
    if pd.notnull(error_msg) and len(error_msg) > 0 and ' - SOLVED' not in error_msg:
        process_monitor.df.at[i, 'Error'] += ' - SOLVED'
    process_monitor.save()
