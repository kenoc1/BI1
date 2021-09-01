import os
from dotenv import load_dotenv

load_dotenv()

DB_CON_USER = os.environ.get('DB_CON_USER')
DB_CON_PW = os.environ.get('DB_CON_PW')