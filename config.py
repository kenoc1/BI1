import os
import cx_Oracle
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

DB_CON_USER_F2 = os.environ.get('DB_CON_USER_F2')
DB_CON_PW_F2 = os.environ.get('DB_CON_PW_F2')
DB_CON_DSN_F2 = cx_Oracle.makedsn(os.environ.get('DB_CON_IP'), 1521, service_name="fastdbwin")

DB_CON_USER_COMBINED = os.environ.get('DB_CON_USER_COMBINED')
DB_CON_PW_COMBINED = os.environ.get('DB_CON_PW_COMBINED')
DB_CON_DSN_COMBINED = cx_Oracle.makedsn(os.environ.get('DB_CON_IP'), 1521, service_name="fastdbwin")

PRODUCTS_F2 = "PRODUKT"
FUNCTIONS_F2 = ["Einkäufer",
                "Oberkassierer*in",
                "Consultant",
                "Raumpfleger*in",
                "Kassierer*in",
                "Praktikant*in",
                "Dualer Student*in",
                "Azubi",
                "Lagerist",
                "Abteilungsleiter*in",
                "Filialleiter*in"]

TIME_FORMAT_F2 = '%m/%d/%Y %I:%M %p'
PAYMENT_METHODS_F2 = ["Bar", "Karte"]
PRODUCT_TYP_F2 = ["gewichtsbasiert", "stueckbasiert"]
PREIS_TYP_F2 = ["Einkauf", "Verkauf"]
PREIS_TYP = ["EINKAUFSPREIS", "LISTENVERKAUFSPREIS"]
DUMMY_MAIL = "nicht vorhanden"
DUMMY_ADDRESS = 12194
GEWICHTSBASIERT_EINHEIT_STUECK = "1kg"
SOURCE_F2 = 2
SOURCE_OS = 1
# DUMMY_DISCOUNT = 0

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
ALLOCATION_CSV_DICT = ROOT_DIR / Path("data/allocation_csvs/")
PRODUCTS_CON_FILE_NAME = ALLOCATION_CSV_DICT / "f2_master_products_con.csv"
PRODUCT_SUB_CAT_CON_FILE_NAME = ALLOCATION_CSV_DICT / "subcategories_ids_old_to_new.csv"
SUPPLIER_CON_FILE_NAME = ALLOCATION_CSV_DICT / "f2_master_lieferant_hersteller_con.csv"
BRAND_CON_FILE_NAME = ALLOCATION_CSV_DICT / "f2_master_marke_con.csv"
