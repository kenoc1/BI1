import os
from pathlib import Path

import cx_Oracle
from dotenv import load_dotenv

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
CSV_FILES_DICT = ROOT_DIR / Path("data/csv-files")
MANUAL_CSV_DICT = ROOT_DIR / Path("data/manual_check/")

PRODUCTS_CON_FILE_NAME = ALLOCATION_CSV_DICT / "f2_master_products_con.csv"
PRODUCT_SUB_CAT_CON_FILE_NAME = ALLOCATION_CSV_DICT / "subcategories_ids_new_to_old.csv"
SUPPLIER_CON_FILE_NAME = ALLOCATION_CSV_DICT / "f2_master_lieferant_hersteller_con.csv"
BRAND_CON_FILE_NAME = ALLOCATION_CSV_DICT / "f2_master_marke_con.csv"
ADDRESS_CON_FILE_NAME = ALLOCATION_CSV_DICT / "adresse.csv"
KUNDEN_CON_FILE_NAME = ALLOCATION_CSV_DICT / "kunden.csv"
MITARBEITER_CON_FILE_NAME = ALLOCATION_CSV_DICT / "mitarbeiter.csv"
EINKAUF_CON_FILE_NAME = ALLOCATION_CSV_DICT / "einkauf.csv"
ZWISCHENHAENDLER_CON_FILE_NAME = ALLOCATION_CSV_DICT / "zwischenhaendler.csv"
BESTELLUNG_CON_FILE_NAME = ALLOCATION_CSV_DICT / "bestellung.csv"
BON_CON_FILE_NAME = ALLOCATION_CSV_DICT / "bon.csv"
RECHNUNG_CON_FILE_NAME = ALLOCATION_CSV_DICT / "rechnung.csv"
LIEFERSCHEIN_CON_FILE_NAME = ALLOCATION_CSV_DICT / "lieferschein.csv"

MANUAL_PRODUCT_SUB_CAT_CON_FILE_NAME = MANUAL_CSV_DICT / "subcategories_ids_new_to_old.csv"

VORNAMEN_GENDER_CSV = CSV_FILES_DICT / "vornamen_gender.csv"

ADDRESS_DB_TABLE = "adresse"
