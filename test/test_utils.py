import unittest
from utils import utils

class TestUtils(unittest.TestCase):
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
                self.assertEqual(utils.get_raw_filename(input_path), expected)