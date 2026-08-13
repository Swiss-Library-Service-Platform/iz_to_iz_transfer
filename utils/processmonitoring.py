import logging
import os
import sys
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd
from pymongo import MongoClient
import pymongo
from pymongo.errors import AutoReconnect, ServerSelectionTimeoutError
from utils import xlstools

class ProcessMonitor:
    """
    Monitors and manages the processing state of a specific process type using a configuration Excel file.

    This class handles the creation, loading, and saving of a process tracking file (CSV) based on the
    provided Excel configuration and process type. It supports different process types (PoLines, Items, Holdings)
    and ensures the correct columns are used for each.

    Parameters
    ----------
    excel_filepath : str
        Path to the Excel file containing the configuration data.
    process_type : str
        Type of process to monitor (e.g., 'PoLines', 'Items', 'Holdings').

    Attributes
    ----------
    excel_filepath : str
        Path to the Excel file containing configuration and process data.
    process_type : str
        The type of process being monitored.
    file_path : str
        Path to the process tracking CSV file.
    df : pandas.DataFrame or None
        DataFrame containing the process data, or None if not loaded/created yet.
    """
    _instance = None
    _mongo_client: Optional[MongoClient] = None

    def __new__(cls, *args, **kwargs):
        """
        Ensures that only one instance of ProcessMonitor is created for a given Excel file and process type.
        """
        if cls._instance is None:
            cls._instance = super(ProcessMonitor, cls).__new__(cls)

        return cls._instance

    def __init__(self, excel_filepath=None, process_type=None) -> None:
        """
        Initializes the ProcessMonitor with the given Excel file path and process type.

        Parameters
        ----------
        excel_filepath : str
            Path to the Excel file containing configuration data.
        process_type : str
            Type of process to monitor.
        """
        if not hasattr(self, '_initialized'):
            config = xlstools.get_config()
            if excel_filepath is None or process_type is None:
                logging.critical("ProcessMonitor requires excel_filepath and process_type for initialization.")
                sys.exit(1)
            self.excel_filepath = excel_filepath
            self.process_type = process_type
            self.file_path = self.get_file_path(excel_filepath)
            self.use_mongodb = config.get('use_mongodb', False)
            if self.use_mongodb:
                self.mongodb_col = self.initiate_mongodb_col()

            self.df = pd.DataFrame({})

            # Check if process data is already existing
            if self.check_existing_data():
                self.load()
            else:
                self.create()

            # Ensure the DataFrame is not None and has data
            if self.df.empty:
                logging.critical(f'Process file {self.file_path} is empty or not loaded correctly.')
                sys.exit(1)

            if not self.use_mongodb:
                self.df.index = range(1, len(self.df) + 1)  # Reset index to start from 1

            self._initialized = True  # Mark the instance as initialized

    def get_file_path(self, excel_filepath: str) -> str:
        """
        Returns the file path for the process file based on the Excel file path and process type.

        Parameters
        ----------
        excel_filepath : str
            Path to the Excel file containing configuration data.

        Returns
        -------
        str
            Path to the process file.
        """
        return f'data/{xlstools.get_raw_filename(excel_filepath)}_{self.process_type}_processing.csv'

    def get_columns(self) -> List[str]:
        """
        Returns the columns of the csv file according to the process type.

        Returns
        -------
        List[str]
            List of column names.
        """
        if self.process_type == 'PoLines':
            return ['PoLine_s', 'MMS_id_s', 'Holding_id_s', 'Item_id_s', 'PoLine_d', 'MMS_id_d', 'Holding_id_d',
                    'Item_id_d', 'Purchase_type', 'Received', 'Copied', 'Error']
        elif self.process_type == 'Items':
            return ['Barcode', 'MMS_id_s', 'Holding_id_s', 'Item_id_s', 'MMS_id_d', 'Holding_id_d', 'Item_id_d', 'Copied', 'Error']
        elif self.process_type == 'Holdings':
            return ['MMS_id_s', 'Holding_id_s', 'MMS_id_d', 'Holding_id_d', 'Copied', 'Error']
        elif self.process_type == 'Bibs':
            return ['MMS_id_s', 'MMS_id_d', 'Copied', 'Error']
        elif self.process_type == 'Collections':
            return ['Collection_id_s', 'Collection_id_d', 'Copied', 'Error']
        elif self.process_type == 'Loans':
            return ['Primary_id', 'Barcode_s', 'MMS_id_s', 'Holding_id_s', 'Item_id_s', 'MMS_id_d', 'Holding_id_d', 'Item_id_d', 'Barcode_d', 'Error']
        elif self.process_type == 'Requests':
            return ['Primary_id', 'Request_id_s', 'Request_id_d', 'Copied', 'Error']
        else:
            logging.critical(f'Unknown process type: {self.process_type}')
            sys.exit(1)

    def _reset_mongo_client(self) -> None:
        if self._mongo_client is not None:
            try:
                self._mongo_client.close()
            finally:
                self._mongo_client = None

    def _get_mongo_client(self) -> MongoClient:
        if self._mongo_client is None:
            uri = os.environ.get('MONGODB_URI_IZ_TO_IZ')
            if not uri:
                logging.critical("Missing env var MONGODB_URI_IZ_TO_IZ")
                sys.exit(1)

            self._mongo_client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                retryWrites=True
            )

        self._mongo_client.admin.command("ping")

        return self._mongo_client


    def initiate_mongodb_col(self) -> pymongo.collection.Collection:
        """

        Returns
        -------
        pymongo.collection.Collection
            MongoDB collection used to store processing rows.
        """
        config = xlstools.get_config()
        db_name = f'{config["iz_d"]}_{config["lib_d"]}'
        col_name = f'{db_name}_{self.process_type}_processing'

        try:
            self._reset_mongo_client()
            client = self._get_mongo_client()
            return client[db_name][col_name]
        except (AutoReconnect, ServerSelectionTimeoutError) as e:
            logging.warning(f"Mongo timeout/reconnect needed: {e}")
            self._reset_mongo_client()
            client = self._get_mongo_client()  # retry unique
            return client[db_name][col_name]
        except Exception as e:
            logging.critical(f"Error connecting to MongoDB: {e}")
            sys.exit(1)

    def check_existing_data(self) -> bool:
        """
        Checks if the process data already exists.

        It checks MongoDB and file data

        Returns
        -------
        bool
            True if the file exists, False otherwise.
        """
        if self.use_mongodb:
            try:
                return self.mongodb_col.find_one({}, {"_id": 1}) is not None
            except Exception as e:
                logging.critical(f"Error connecting to MongoDB: {e}")
                sys.exit(1)
        else:
            return os.path.isfile(self.file_path)

    def create(self) -> None:
        """
        Creates a new process file with the appropriate columns.
        """
        cols = self.get_columns()
        self.df = pd.DataFrame(columns=cols)
        os.makedirs('data', exist_ok=True)
        self.load_data_from_excel()
        if self.df is not None:
            self.df['Copied'] = False
        self.save()

    def load(self) -> None:
        """
        Loads the existing process file into a DataFrame.
        """
        columns = self.get_columns()
        dtype_dict = {column: 'boolean' if column in ['Copied', 'Received'] else 'string' for column in columns}

        if self.use_mongodb:
            data = [row for row in self.mongodb_col.find({})]
            dtype_dict['Rank'] = 'int'
            columns.append('Rank')

            self.df = pd.DataFrame(data=data, columns=columns)

            for col, dtype in dtype_dict.items():
                if col in self.df.columns:
                    self.df[col] = self.df[col].astype(dtype)
            self.df.set_index('Rank', inplace=True)
            self.load_data_from_excel()
            self.save()

        else:

            try:
                self.df = pd.read_csv(self.file_path, dtype=dtype_dict)
            except FileNotFoundError:
                logging.critical(f"File not found: {self.file_path}")
                sys.exit(1)
            except pd.errors.ParserError as e:
                logging.critical(f"CSV parsing error: {self.file_path}: {e}")
                sys.exit(1)
            except ValueError as e:
                logging.critical(f"Data type error (dtype): {self.file_path}: {e}")
                sys.exit(1)

    def save(self, rank: Optional[int] = None) -> None:
        """
        Saves the current DataFrame to the process file.
        """
        if self.use_mongodb:
            save_date = datetime.now(timezone.utc)
            if rank is None:
                self.df.index = range(1, len(self.df) + 1)  # Reset index to start from 1

                records_df = self.df.copy()
                if records_df.index.name != 'Rank':
                    records_df.index.name = 'Rank'

                # One DataFrame row -> one MongoDB document.
                records_df = records_df.reset_index()
                records_df['date'] = save_date
                records = records_df.to_dict(orient='records')
                records = [
                    {k: (None if pd.isnull(v) else v) for k, v in record.items()}
                    for record in records
                ]

                # Keep save() semantics close to CSV overwrite.
                self.mongodb_col.delete_many({})
                if records:
                    self.mongodb_col.insert_many(records)
            else:
                record = self.df.loc[rank].to_dict()
                record['Rank'] = rank
                record = {k: (v if pd.notnull(v) else None) for k, v in record.items()}
                record['date'] = save_date
                try:
                    self.mongodb_col.replace_one({'Rank': rank}, record)
                except Exception as e:
                    logging.error(f"Record with rank {rank} not found, error: {e}")

        else:
            self.df.to_csv(self.file_path, index=False)

    def _update_many_mongodb(self, source_field: str, source_value: str, payload: dict) -> None:
        """Applies a MongoDB update_many when MongoDB backend is enabled."""
        if not self.use_mongodb:
            return

        try:
            self.mongodb_col.update_many({source_field: source_value}, {'$set': payload})
        except Exception as e:
            logging.error(
                f"MongoDB bulk update failed on {source_field}={source_value} with payload {payload}: {e}"
            )

    def load_data_from_excel(self) -> None:
        """
        Loads data from the specified Excel file into the DataFrame for processing.

        This method reads the Excel file at the path specified by `self.excel_filepath`,
        using the sheet named after the current process type.
        It aligns the columns of the loaded data with those expected for the process type,
        appends the data to the existing DataFrame, and initializes the 'Copied' column to False for all rows.

        No parameters are required.
        """
        data = pd.read_excel(self.excel_filepath, sheet_name=self.process_type, dtype=str)
        data.columns = self.get_columns()[:len(data.columns)]
        self.df = pd.concat([self.df, data], ignore_index=True)
        self.df = self.df.drop_duplicates(subset=data.columns.tolist(), keep='first')
        if 'Copied' in self.df.columns:
            self.df['Copied'] = self.df['Copied'].fillna(False)



    def get_corresponding_poline(self, pol_number: str) -> tuple[str | None, str | None]:
        """
        Returns the destination PoLine number and purchase type for the given source PoLine number.

        Parameters
        ----------
        pol_number : str
            The source PoLine number to search for.

        Returns
        -------
        tuple[str | None, str | None]
            The destination PoLine number and purchase type if found, otherwise None.
        """
        if self.process_type != 'PoLines':
            logging.critical(f'Process type {self.process_type} does not support getting PoLine data.')
            sys.exit(1)

        result = self.df.loc[self.df['PoLine_s'] == pol_number, ['PoLine_d', 'Purchase_type']]
        if len(result) > 0:
            poline_d = result['PoLine_d'].values[0]
            purchase_type = result['Purchase_type'].values[0]
            if pd.isnull(poline_d):
                return None, None
            return poline_d, purchase_type
        else:
            return None, None

    def get_corresponding_mms_id(self, mms_id: str) -> str | None:
        """
        Returns the DataFrame row corresponding to the given MMS ID.

        Parameters
        ----------
        mms_id : str
            The MMS ID to search for.

        Returns
        -------
        str | None
            The corresponding MMS ID if found, otherwise None.
        """
        result = self.df.loc[self.df['MMS_id_s'] == mms_id, 'MMS_id_d']
        value = result.values[0] if len(result) > 0 else None
        return None if pd.isnull(value) else value

    def get_corresponding_holding_id(self, holding_id: str) -> str | None:
        """
        Returns the DataFrame row corresponding to the given Holding ID.

        Parameters
        ----------
        holding_id : str
            The Holding ID to search for.

        Returns
        -------
        str | None
            The corresponding Holding ID if found, otherwise None.
        """

        result = self.df.loc[self.df['Holding_id_s'] == holding_id, 'Holding_id_d']
        value = result.values[0] if len(result) > 0 else None
        return None if pd.isnull(value) else value

    def get_corresponding_item_id(self, item_id: str) -> str | None:
        """
        Returns the DataFrame row corresponding to the given Item ID.

        Parameters
        ----------
        item_id : str
            The Item ID to search for.

        Returns
        -------
        str | None
            The corresponding Item ID if found, otherwise None.
        """

        result = self.df.loc[self.df['Item_id_s'] == item_id, 'Item_id_d']
        value = result.values[0] if len(result) > 0 else None
        return None if pd.isnull(value) else value

    def set_corresponding_poline(self, pol_number: str, poline_d: str, purchase_type: str) -> None:
        """
        Sets the destination PoLine number for all rows matching the given source PoLine number.

        Parameters
        ----------
        pol_number : str
            The source PoLine number to match.
        poline_d : str
            The destination PoLine number to set.
        purchase_type : str
            The purchase type to set.
        """
        if self.process_type != 'PoLines':
            logging.critical(f'Process type {self.process_type} does not support setting PoLine data.')
            sys.exit(1)

        self.df.loc[self.df['PoLine_s'] == pol_number, 'PoLine_d'] = poline_d
        self.df.loc[self.df['PoLine_s'] == pol_number, 'Purchase_type'] = purchase_type
        self._update_many_mongodb('PoLine_s', pol_number, {'PoLine_d': poline_d, 'Purchase_type': purchase_type})

    def set_corresponding_mms_id(self, mms_id_s: str, mms_id_d: str) -> None:
        """
        Sets the destination MMS ID for all rows matching the given source MMS ID.

        Parameters
        ----------
        mms_id_s : str
            The source MMS ID to match.
        mms_id_d : str
            The destination MMS ID to set.
        """
        self.df.loc[self.df['MMS_id_s'] == mms_id_s, 'MMS_id_d'] = mms_id_d
        self._update_many_mongodb('MMS_id_s', mms_id_s, {'MMS_id_d': mms_id_d})

    def set_corresponding_holding_id(self, holding_id_s: str, holding_id_d: str) -> None:
        """
        Sets the destination Holding ID for all rows matching the given source Holding ID.

        Parameters
        ----------
        holding_id_s : str
            The source Holding ID to match.
        holding_id_d : str
            The destination Holding ID to set.
        """
        self.df.loc[self.df['Holding_id_s'] == holding_id_s, 'Holding_id_d'] = holding_id_d
        self._update_many_mongodb('Holding_id_s', holding_id_s, {'Holding_id_d': holding_id_d})

    def set_corresponding_item_id(self, item_id_s: str, item_id_d: str) -> None:
        """
        Sets the destination Item ID for all rows matching the given source Item ID.

        Parameters
        ----------
        item_id_s : str
            The source Item ID to match.
        item_id_d : str
            The destination Item ID to set.
        """
        self.df.loc[self.df['Item_id_s'] == item_id_s, 'Item_id_d'] = item_id_d
        self._update_many_mongodb('Item_id_s', item_id_s, {'Item_id_d': item_id_d})

    @classmethod
    def reset(cls):
        """
        Resets the singleton instance of ProcessMonitor.

        This method is useful for testing purposes to ensure a fresh instance is created.
        """
        cls._instance = None
        logging.info("ProcessMonitor instance reset.")
