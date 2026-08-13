from dotenv import load_dotenv
from pathlib import Path
import os

EXCEL_FORM_VERSION = '8.0'

def load_env():
    if 'alma_api_keys' not in os.environ or 'MONGODB_URI_IZ_TO_IZ' not in os.environ:
        dotenv_path = Path(__file__).resolve().parent.parent / '.env'
        load_dotenv(dotenv_path=dotenv_path)

