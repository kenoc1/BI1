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


TIME_FORMAT_F2 = '%m/%d/%Y %I:%M %p'
PAYMENT_METHODS_F2 = ["Bar", "Karte"]
PRODUCT_TYP_F2 = ["gewichtsbasiert", "stueckbasiert"]
PREIS_TYP_F2 = ["Einkauf", "Verkauf"]
PREIS_TYP = ["EINKAUFSPREIS", "LISTENVERKAUFSPREIS"]
DUMMY_MAIL = "nicht vorhanden"
DUMMY_ADDRESS = 12194
DUMMY_KUNDE = 6435
DUMMY_EAN = "nicht vorhanden"
# DUMMY_DISCOUNT = 0

GEWICHTSBASIERT_EINHEIT_STUECK = "1kg"
SOURCE_F2 = 2
SOURCE_OS = 1
F2_VERKAUFSFLAECHE_LAGER_ID = 32
F2_LAGERFLAECHE_LAGER_ID = 31


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
ALLOCATION_CSV_DICT = ROOT_DIR / Path("data/allocation_csvs/")
CSV_FILES_DICT = ROOT_DIR / Path("data/csv-files")
MANUAL_CSV_DICT = ROOT_DIR / Path("data/manual_check/")
VORNAMEN_GENDER_CSV = CSV_FILES_DICT / "vornamen_gender.csv"
RUDOLF_DB_PATH = ROOT_DIR / Path("rudolf-db.db")
RUDOLF_TEST_DB_PATH = ROOT_DIR / Path("test-rudolf-db.db")

ADDRESS_TABLE = "adresse"
LIEFERANT_HERSTELLER_TABLE = "lieferant_hersteller"
WORKER_POSITION_TABLE = "funktion"
WORKER_TABLE = "mitarbeiter"
LAGERPLATZ_TABLE = "lagerplatz"
KUNDEN_TABLE = "kunden"
PRODUKTE_TABLE = "produkte"
BESTELLUNG_TABLE = "bestellung"
SUBKATEGORIE_TABLE = "subkategorien"
MARKE_TABLE = "marke"
PREIS_TABLE = "preis"
ZWHAENDLER_TABLE = "zwischenhaendler"
EINKAUF_TABLE = "einkauf"
