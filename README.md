# Transfer IZ to IZ
This script is used to copy bib records, holdings and
items from one IZ to another one IZ. The IZ need to be link
to the same NZ.

* Author: Raphaël Rey (raphael.rey@slsp.ch)
* Year: 2022
* Version: 0.19.1
* License: GNU General Public License v3.0

## Usage
```bash
python3 transfer_iz_to_iz.py <form_iz_to_iz2>.xlsx
```

A model of the Excel document is provided in the models folder. The system need to have:
* Basic configuration with IZ source and destination + decision to accept deletion of some fields in the items
* List of barcodes to transfer
* Mapping of locations
* Mapping of item policies

Default values can be provided for locations and item policies (row with `*DEFAULT*`) and will be used if no other
mapping is found.

## Detailed process
The script will:
1. Fetch source item
2. Fetch source holding
3. Fetch source bib
4. Create destination bib if required
5. Create destination holding if required
6. Create destination item
7. Update barcode source item adding `OLD_` prefix

## Resilience to errors
The script stores all actions in log files and use a csv file to keep
a view of the current state of the work. For this reason it is possible
to run the script multiple times without any risk.

## Produced files
* Log files in the `logs` folder
* csv files with state of the work:
  * <form_iz_to_iz2>_processing.csv: list of item, holdings and bib records in both IZ. Error will be indicated here too.
  * <form_iz_to_iz2>_not_copied.csv: list with only errors

## Requirements
- Python 3.9 or higher
- [Almapiwrapper](https://almapi-wrapper.readthedocs.io/en/latest/) configured with IZ api keys. No NZ key is required.

