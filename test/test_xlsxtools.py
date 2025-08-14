import unittest
from utils import xlstools

import pandas as pd

class TestXlsTools(unittest.TestCase):

    def test_get_form_version(self):
        excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
        version = xlstools.get_form_version(excel_path)
        self.assertGreater(float(version), 4.0)
        self.assertLess(float(version), 9.0)

    def test_get_config(self):
        excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
        xlstools.set_config(excel_path)
        self.assertIsInstance(xlstools.get_config(), dict)
        self.assertEqual(xlstools.get_config().get('iz_s'), 'UBS')
        self.assertEqual(xlstools.get_config().get('iz_d'), 'ISR')
        self.assertEqual(xlstools.get_config().get('env'), 'S')
        self.assertEqual(xlstools.get_config().get('acq_department'), 'DEFAULT_CIRC_DESK-AcqWorkOrder')
        self.assertEqual(xlstools.get_config().get('make_reception'), False)
        self.assertEqual(xlstools.get_config().get('items_fields'), {'src': {'to_delete': ['temp_location', 'temp_library', 'in_temp_location'], 'to_delete_if_error': ['provenance', 'pattern_type', 'statistics_note_1', 'statistics_note_2', 'statistics_note_3']}, 'dest': {'to_delete': ['temp_location', 'temp_library', 'in_temp_location'], 'to_delete_if_error': ['provenance', 'statistics_note_1', 'statistics_note_2', 'statistics_note_3']}})
        self.assertEqual(xlstools.get_config().get('polines_fields'), {'to_delete': [], 'to_delete_if_error': ['interested_user']})
        self.assertIn('vendors_mapping', xlstools.get_config())
        self.assertEqual(xlstools.get_config()['locations_mapping'].iloc[0, 0], '*DEFAULT*')
        self.assertEqual(xlstools.get_config().get('circ_desk_s'), 'A100_KUR')
        self.assertEqual(xlstools.get_config().get('circ_desk_d'), 'DEFAULT_CIRC_DESK')
        self.assertIsNone(xlstools.get_config().get('cancel_note'))

    def test_polines_to_transfer(self):
        excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
        polines = xlstools.get_data(excel_path, 'PoLines')
        self.assertIsInstance(polines, pd.DataFrame)
        self.assertGreater(len(polines), 0)
        self.assertEqual('POL-UBS-2025-167396', polines.iloc[0]['PO Line Reference'])

    def test_get_corresponding_locations(self):
        excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
        xlstools.set_config(excel_path)
        lib, loc = xlstools.get_corresponding_location('A100', 'MAG')
        self.assertEqual(lib, 'rro_fili')
        self.assertEqual(loc, '610940001')

    def test_get_corresponding_library(self):
        excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
        xlstools.set_config(excel_path)
        lib = xlstools.get_corresponding_library('A100')
        self.assertEqual(lib, 'rro_fili')

    # def test_get_corresponding_item_policy(self):
    #     excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
    #     xlstools.set_config(excel_path)
    #     item_policy = xlstools.get_corresponding_item_policy('08')
    #     self.assertEqual(item_policy, '01')

    def test_get_corresponding_vendor(self):
        excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
        xlstools.set_config(excel_path)
        vendor, vendor_account = xlstools.get_corresponding_vendor('ABC_vendor', '12345')
        self.assertEqual(vendor, '000007023')
        self.assertEqual(vendor_account, '000007023')

        vendor, vendor_account = xlstools.get_corresponding_vendor('A100-1044', 'zzzzzzz')
        self.assertEqual(vendor, '000000015')
        self.assertEqual(vendor_account, '000000015')

        vendor, vendor_account = xlstools.get_corresponding_vendor('A100-1045', 'zzzzzzz')
        self.assertIsNone(vendor)
        self.assertIsNone(vendor_account)

    def test_get_corresponding_fund(self):
        excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
        xlstools.set_config(excel_path)
        fund = xlstools.get_corresponding_fund('test')
        self.assertEqual(fund, 'Fundforall')

    def test_create_log_filename(self):
        test_cases = [
            ("log.txt", "log"),
            ("C:\\Users\\user\\log.txt", "log"),
            ("/home/user/log.txt", "log"),
            ("archive.tar.gz", "archive.tar"),
            ("folder/subfolder/file.log", "file"),
            ("file", "file"),
        ]
        for input_path, expected in test_cases:
            with self.subTest(input_path=input_path):
                self.assertEqual(xlstools.get_raw_filename(input_path), expected)

if __name__ == '__main__':
    unittest.main()