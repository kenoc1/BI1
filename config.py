import os
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
DUMMY_MAIL = "nicht vorhanden"
DUMMY_ADDRESS = 0
