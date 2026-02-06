from typing import Optional
from almapiwrapper.inventory import IzBib, NzBib, Holding, Item, Collection
from utils import xlstools
from utils.processmonitoring import ProcessMonitor

import logging

config = xlstools.get_config()


def copy_bib_from_nz_to_dest_iz(iz_mms_id_s: str) -> Optional[IzBib]:
    """
    Copies a bib record from the NZ to the destination IZ, using the source IZ MMS ID.
    If the source IZ bib is not linked to the NZ, it creates a new bib in the destination IZ.

    Parameters
    ----------
    iz_mms_id_s : str
        The MMS ID of the bib in the source IZ

    Returns
    -------
    IzBib, Optional
        The IzBib object of the destination IZ, or None if an error occurs.
    """

    process_monitor = ProcessMonitor()

    # We fetch source IZ Bib to get the NZ MMS ID
    iz_bib_s = IzBib(iz_mms_id_s, zone=config['iz_s'], env=config['env'])
    nz_mms_id = iz_bib_s.get_nz_mms_id()

    if iz_bib_s.error:
        logging.error(f"{repr(iz_bib_s)}: {iz_bib_s.error_msg}")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == iz_mms_id_s, 'Error'] = 'Source IZ Bib not found'
        return None

    # We make a copy of the local source record if it is not linked to the NZ
    if nz_mms_id is None:
        logging.error(f"{repr(iz_bib_s)}: not linked to the NZ")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == iz_mms_id_s, 'Error'] = 'Not linked to the NZ'
        process_monitor.save()
        iz_bib_d = IzBib(data=iz_bib_s.data, zone=config['iz_d'], env=config['env'], create_bib=True)
    else:
        # We copy the NZ Bib to the destination IZ
        iz_bib_d = IzBib(nz_mms_id, zone=config['iz_d'], env=config['env'], from_nz_mms_id=True, copy_nz_rec=True)

    if iz_bib_d.error:
        logging.error(f"{repr(iz_bib_d)}: {iz_bib_d.error_msg}")
        process_monitor.df.loc[process_monitor.df['MMS_id_s'] == iz_mms_id_s, 'Error'] = 'Destination IZ Bib not created'
        process_monitor.save()
        return None

    # Copy local extensions
    i = process_monitor.df.index[process_monitor.df['MMS_id_s'] == iz_mms_id_s][0]
    iz_bib_d = copy_local_extensions(iz_bib_s,
                                     iz_bib_d,
                                     i)

    return iz_bib_d


def get_corresponding_bib_from_col(iz_bib_s: IzBib, i: int) -> Optional[IzBib]:
    """
    Retrieves the corresponding IzBib object from the destination IZ based on the source IzBib object.
    If the source IzBib is not linked to the NZ, it creates a new IzBib in the destination IZ.
    If the source IzBib is linked to the NZ, it copies the NZ record to the destination IZ.
    If an error occurs, it logs the error and updates the process monitor DataFrame.

    Parameters
    ----------
    iz_bib_s : IzBib
        The IzBib object of the source IZ.
    i : int
        The index of the row in the process monitor DataFrame.

    Returns
    -------
    IzBib
        The IzBib object of the destination IZ, or None if an error occurs.
    """
    process_monitor = ProcessMonitor()

    # We fetch source IZ Bib to get the NZ MMS ID
    nz_mms_id = iz_bib_s.get_nz_mms_id()

    if iz_bib_s.error:
        logging.error(f"{repr(iz_bib_s)}: {iz_bib_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Source IZ Bib not found'
        process_monitor.save()
        return None

    # We make a copy of the local source record if it is not linked to the NZ
    if nz_mms_id is None:
        logging.error(f"{repr(iz_bib_s)}: not linked to the NZ")
        process_monitor.df.at[i, 'Error'] = 'Not linked to the NZ'
        iz_bib_d = IzBib(data=iz_bib_s.data, zone=config['iz_d'], env=config['env'], create_bib=True)
    else:
        # We copy the NZ Bib to the destination IZ
        iz_bib_d = IzBib(nz_mms_id, zone=config['iz_d'], env=config['env'], from_nz_mms_id=True, copy_nz_rec=True)

    if iz_bib_d.error:
        logging.error(f"{repr(iz_bib_d)}: {iz_bib_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Destination IZ Bib not created'
        return None

    # Copy local extensions
    iz_bib_d = copy_local_extensions(iz_bib_s,
                                     iz_bib_d,
                                     i)

    return iz_bib_d


def copy_local_extensions(iz_bib_s: IzBib, iz_bib_d: IzBib, i: int) -> Optional[IzBib]:
    """
    Copies local extensions (specific 998 fields) from the source IZ record (iz_bib_s) to the destination IZ record (iz_bib_d).

    For each 998 field in the source record whose subfield $a contains one of the specific values
    ('no_inventory_analytical', 'no_inventory_superordinate_monograph'), this field is added to the destination record.

    If no local extension is found, the destination record is returned unchanged.
    In case of an error during the copy or update, the error is logged and the function returns None.

    Parameters
    ----------
    iz_bib_s : IzBib
        Source IZ record.
    iz_bib_d : IzBib
        Destination IZ record.
    i : int
        Index of the row in the process monitor DataFrame (for error tracking).

    Returns
    -------
    Optional[IzBib]
        The updated destination IZ record, or None in case of error.
    """
    process_monitor = ProcessMonitor()

    # We copy local extensions from source IZ bib to destination IZ bib
    f998s = [f998 for f998 in iz_bib_s.data.findall('.//datafield[@tag="998"]')
             if f998.find('./subfield[@code="a"]') is not None
             and f998.find('./subfield[@code="a"]').text in
             [ 'no_inventory_analytical', 'no_inventory_superordinate_monograph', 'no_inventory_analytical']]

    if len(f998s) == 0:
        return iz_bib_d

    mms_id_d = iz_bib_d.get_mms_id()
    iz_bib_d = IzBib(mms_id_d, zone=config['iz_d'], env=config['env'])
    _ = iz_bib_d.data

    if iz_bib_d.error:
        logging.error(f"{repr(iz_bib_d)}: {iz_bib_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Local extensions not copied'
        return None

    for f998 in f998s:
        iz_bib_d.data.find('.//record').append(f998)

    iz_bib_d.sort_fields().update()
    if iz_bib_d.error:
        logging.error(f"{repr(iz_bib_d)}: {iz_bib_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Local extensions not copied'
        return None
    logging.info(f"{repr(iz_bib_d)}: {len(f998s)} local extensions copied")
    return iz_bib_d
