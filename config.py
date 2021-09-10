import os
import cx_Oracle
from dotenv import load_dotenv

load_dotenv()

DB_CON_USER = os.environ.get('DB_CON_USER')
DB_CON_PW = os.environ.get('DB_CON_PW')
DB_CON_DSN = cx_Oracle.makedsn(os.environ.get('DB_CON_IP'), 1521, service_name="fastdbwin")

PRODUCTS = "PRODUKT"
FUNCTIONS = ["Einkäufer",
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

TIME_FORMAT = '%m/%d/%Y %I:%M %p'
PAYMENT_METHODS = ["Bar", "Karte"]
