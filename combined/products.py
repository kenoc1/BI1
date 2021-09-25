import config
import util
from db_service import DB_F2, DB_MASTER
import numpy as np


# --- OS PRODUKT---
# PRODUKT_ID NUMBER primary key,
# LIEFERANT_ID NUMBER
# PRODUKTKLASSE_ID NUMBER
# PROUKT_NAME VARCHAR2(150) not null,
# EAN VARCHAR2(50) not null,
# SKU VARCHAR2(50) not null,
# PRODUKTBESCHREIBUNG VARCHAR2(500),
# ANGEBOTSRABATT FLOAT not null check (angebotsrabatt between 0 and 1),
# EINHEITSGROESSE VARCHAR2(30),
# EINKAUFSPREIS FLOAT not null,
# LISTENVERKAUFSPREIS FLOAT not null,
# RANKING FLOAT,
# RECYCLEBAR NUMBER(1),
# LOW_FAT NUMBER(1),
# BEWERTUNG FLOAT,
# MWST_SATZ FLOAT not null	check (mwst_satz between 0 and 1)


# --- F2 PRODUKT ---
# Produkt_Id       NUMBER(10),
# Gewicht          NUMBER(10, 4) CHECK (Gewicht >= '0'),
# Umsatzsteuersatz NUMBER(10, 2)      NOT NULL CHECK (Umsatzsteuersatz >= '0'),
# Bezeichnung      VARCHAR2(128 CHAR) NOT NULL,
# SKU              NUMBER(10)         NOT NULL CHECK (SKU >= '0'),
# Typ              VARCHAR2(15 CHAR) CHECK ( Typ IN ('gewichtsbasiert', 'stueckbasiert')),
# Produkt_Hoehe    NUMBER(10, 2),
# Produkt_Tiefe    NUMBER(10, 2),
# Produkt_Breite   NUMBER(10, 2),
# Marke_Id         NUMBER(10)         NOT NULL,

def save_f2_master_products_id_connection(connections):
    a = np.array(connections)
    np.savetxt('f2_master_lieferant_hersteller_con.csv', a, delimiter=',')


class Products:
    def __init__(self):
        self.db_f2 = DB_F2()
        self.db_master = DB_MASTER()

    def insert_products_from_f2_to_master(self):
        return

    def get_supplier_id_with_brand_id(self):
        return
