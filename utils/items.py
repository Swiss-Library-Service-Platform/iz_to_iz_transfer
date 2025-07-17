from typing import Optional
from almapiwrapper.inventory import IzBib, NzBib, Holding, Item, Collection

from utils import xlstools
from utils.processmonitoring import ProcessMonitor
from copy import deepcopy
from lxml import etree

import logging

def copy_item_to_the_destination_iz(i, poline: Optional[bool]) -> None:
    """
    Copies an item from the source IZ to the destination IZ based on the provided index and configuration.
    This function retrieves the source item, updates its library and location according to the destination IZ,
    and creates a new item in the destination IZ. If the item is associated with a PoLine, it updates the PoLine
    information as well.

    Parameters
    ----------
    i : int
        The index of the row to process in the DataFrame.
    poline : Optional[bool]
        If True, the function will also handle the PoLine information for the item.

    Returns
    -------
    None
    """
    # Load configuration and initialize process monitor
    config = xlstools.get_config()
    process_monitor = ProcessMonitor()

    # Get the source and destination MMS IDs
    mms_id_s = process_monitor.df.at[i, 'MMS_id_s']
    mms_id_d = process_monitor.get_corresponding_mms_id(mms_id_s)

    # Get the source holding ID
    holding_id_s = process_monitor.df.at[i, 'Holding_id_s']
    holding_id_d = process_monitor.get_corresponding_holding_id(holding_id_s)

    # Fetch the source item
    item_id_s = process_monitor.df.at[i, 'Item_id_s']

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
        received = False if arrival_date is None else True
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
    process_monitor.save()

    update_source_item(item_s)

    if item_s.error:
        logging.error(f"{repr(item_s)}: failed to update barcode of source record: {item_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Failed to update source item barcode'
        return None


def clean_item_fields(item_data: etree.Element, rec_loc: str, retry=False) -> etree.Element:
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
    config = xlstools.get_config()

    fields_to_remove = deepcopy(config['items_fields'][rec_loc]['to_delete'])

    if retry:
        fields_to_remove += config['items_fields'][rec_loc]['to_delete_if_error']

    for field in fields_to_remove:
        field_element = item_data.find(f".//{field}")

        if field_element is not None:
            field_element.getparent().remove(field_element)

    return item_data

def update_source_item(item_s: Item) -> None:
    """
    Updates the source item by removing the 'OLD_' prefix from the barcode.

    Parameters
    ----------
    i : int
        The index of the row to process in the DataFrame.

    Returns
    -------
    None
    """

    # # Load configuration and initialize process monitor
    config = xlstools.get_config()
    process_monitor = ProcessMonitor()

    # Change barcode of source item
    if item_s.barcode.startswith('OLD_'):
        # Skip this step if barcode already updated
        logging.warning(f'{repr(item_s)}: barcode already updated "{item_s.barcode}"')
        return

    item_s.save()
    item_s.barcode = 'OLD_' + item_s.barcode

    item_s.update()
