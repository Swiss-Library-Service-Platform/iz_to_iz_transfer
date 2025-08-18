import logging
from typing import Optional
from copy import deepcopy
from datetime import timedelta

import pandas as pd
from almapiwrapper.inventory import Item, IzBib
from almapiwrapper.users import User, Request
from almapiwrapper.record import JsonData
from datetime import datetime

from utils import xlstools
from utils.processmonitoring import ProcessMonitor

config = xlstools.get_config()

def create_request(i: int, request_s: Request) -> Optional[Request]:
    """
    Creates a request in the destination IZ based on the provided Request object.

    Parameters
    ----------
    i : int
        The index of the row in the process monitor DataFrame to process.
    request_s : Request
        The Request object containing the details of the request to be created.

    Returns
    -------
    Optional[Request]
        The created Request object if successful, or None if an error occurs.
    """
    process_monitor = ProcessMonitor()

    bib_s = IzBib(request_s.data['mms_id'], config['iz_s'], env=config['env'])
    nz_mms_id = bib_s.get_nz_mms_id()

    if bib_s.error:
        logging.error(f"{repr(bib_s)}: {bib_s.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'NZ MMS ID not found'
        process_monitor.save()
        return None

    if nz_mms_id is None:
        logging.error(f"NZ MMS ID not found for {request_s.data['mms_id']}")
        process_monitor.df.at[i, 'Error'] = 'NZ MMS ID not found'
        process_monitor.save()
        return None

    bib_d = IzBib(nz_mms_id, zone=config['iz_d'], env=config['env'], from_nz_mms_id=True)
    mms_id_d = bib_d.mms_id

    if bib_d.error:
        logging.error(f"{repr(bib_d)}: {bib_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Destination IZ Bib not found'
        process_monitor.save()
        return None

    data = deepcopy(request_s.data)
    data['pickup_location_library'] = config['lib_d']

    if 'barcode' in data:
        item_d = Item(barcode=data['barcode'].replace('OLD_', ''), zone=config['iz_d'], env=config['env'])
        _ = item_d.data

        if item_d.error:
            logging.error(f"{repr(item_d)}: {item_d.error_msg}")
            process_monitor.df.at[i, 'Error'] = 'Item not found'
            process_monitor.save()
            return None

        new_iz_item_id = item_d.item_id
        data['barcode'] = data['barcode'].replace('OLD_', '')
        data['item_id'] = new_iz_item_id

    data['mms_id'] = mms_id_d

    # Warning: request status case must be changed
    data['request_status'] = 'Not Started'
    del data['request_id']
    if 'booking_start_date' in data:
        start_date = datetime.strptime(data['booking_start_date'], '%Y-%m-%dT%H:%M:%SZ')
        now = datetime.utcnow()
        if start_date < now:
            logging.warning(f"Booking start date {start_date} is in the past for request {data.get('request_id', 'unknown')}. Setting to now.")
            data['booking_start_date'] = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    if 'adjusted_booking_start_date' in data:
        del data['adjusted_booking_start_date']

    if 'adjusted_booking_start_date' in data:
        del data['adjusted_booking_end_date']

    request_d = Request(data=JsonData(data), zone=config['iz_d'], env=config['env']).create()

    # Second try to create the booking request if the first attempt failed
    if request_d.error and 'booking_start_date' in data:
        start_date = datetime.strptime(data["booking_start_date"], "%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.strptime(data["booking_end_date"], "%Y-%m-%dT%H:%M:%SZ")
        max_end_date = start_date + timedelta(days=27)

        if end_date > max_end_date:
            data["booking_end_date"] = max_end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            request_d = Request(data=JsonData(data), zone=config['iz_d'], env=config['env']).create()
            process_monitor.df.at[i, 'Error'] = 'Booking end date adjusted'
            process_monitor.save()
            logging.warning(f"{repr(request_d)}: 'Booking end date adjusted: from {end_date.strftime("%Y-%m-%dT%H:%M:%SZ")} to {data["booking_end_date"]}'")

    if request_d.error:
        logging.error(f"{repr(request_d)}: {request_d.error_msg}")
        process_monitor.df.at[i, 'Error'] = 'Request creation failed'
        process_monitor.save()
        return None

    return request_d

