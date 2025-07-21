from utils import xlstools
excel_path = 'test/test_data/test_data_IZ_to_IZ_1.xlsx'
xlstools.set_config(excel_path)
config = xlstools.get_config()

import unittest
from almapiwrapper.inventory import Item
from almapiwrapper.record import XmlData
from utils import items
from copy import deepcopy
from lxml import etree



class TestItems(unittest.TestCase):
    def test_clean_item_fields(self):
        xml_data = XmlData(filepath='test/test_data/item_22434853660005504_23454312290005504_01.xml')

        item = Item(data=xml_data)
        cleaned_item_data = items.clean_item_fields(deepcopy(item.data), rec_loc='src', retry=False)
        pattern_type = cleaned_item_data.find('.//item_data/pattern_type')
        self.assertIsNotNone(pattern_type, 'Pattern type should not be removed ("delete if error")')

        cleaned_item_data = items.clean_item_fields(deepcopy(item.data), rec_loc='src', retry=True)
        pattern_type = cleaned_item_data.find('.//item_data/pattern_type')
        self.assertIsNone(pattern_type, 'Pattern type should be removed ("delete if error")')

        cleaned_item_data = items.clean_item_fields(deepcopy(item.data), rec_loc='dest', retry=True)
        pattern_type = cleaned_item_data.find('.//item_data/pattern_type')
        self.assertIsNotNone(pattern_type, 'Pattern type should not be removed ("delete if error")')
