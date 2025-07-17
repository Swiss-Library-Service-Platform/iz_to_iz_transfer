import os
import pandas as pd
import unittest
from unittest import mock
from utils.processmonitoring import ProcessMonitor


class TestProcessMonitor(unittest.TestCase):
    def setUp(self):
        ProcessMonitor.reset()
        self.pm = ProcessMonitor('test/test_data/test_data_IZ_to_IZ_1.xlsx', "PoLines")
        self.pm.save()

    def tearDown(self):
        import shutil
        shutil.rmtree('data', ignore_errors=True)

    def test_get_file_path(self):
        expected = "data/test_data_IZ_to_IZ_1_PoLines_processing.csv"
        self.assertEqual(self.pm.file_path, expected)

    def test_get_columns_polines(self):
        cols = self.pm.get_columns()
        self.assertEqual(cols, ['PoLine_s', 'MMS_id_s', 'Holding_id_s', 'Item_id_s', 'PoLine_d', 'MMS_id_d', 'Holding_id_d',
                                'Item_id_d', 'Purchase_type', 'Received', 'Copied', 'Error'])

    def test_create_and_save(self):
        self.assertIsInstance(self.pm.df, pd.DataFrame)
        self.assertEqual(list(self.pm.df.columns), self.pm.get_columns())
        self.assertTrue(os.path.isfile('data/test_data_IZ_to_IZ_1_PoLines_processing.csv'))

    def test_load_existing_file(self):
        self.pm.save()
        self.pm.reset()
        self.pm2 = ProcessMonitor('test/test_data/test_data_IZ_to_IZ_1.xlsx', "PoLines")
        self.assertTrue(self.pm2.check_existing_file())
        self.assertIsInstance(self.pm2.df, pd.DataFrame)
        self.assertEqual(self.pm2.df.iloc[0]['MMS_id_s'], '9972798270405504')

    def test_set_and_get_data(self):
        self.assertEqual(self.pm.get_corresponding_poline('POL-UBS-2025-167396'), (None, None))
        self.pm.set_corresponding_poline('POL-UBS-2025-167396', 'POL-ISR-2025-167388', 'PRINTED_BOOK_OT')
        self.assertEqual(self.pm.get_corresponding_poline('POL-UBS-2025-167396'), ('POL-ISR-2025-167388', 'PRINTED_BOOK_OT'))

if __name__ == "__main__":
    unittest.main()