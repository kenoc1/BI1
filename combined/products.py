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


def convert_mwst(mwst: float):
    return mwst / 100


def get_new_product_class_id(self, f2_class_id: str):
    return True


class Products:
    def __init__(self):
        self.db_f2 = DB_F2()
        self.db_master = DB_MASTER()

    def insert_products_from_f2_to_master(self):
        f2_master_products_connection = []
        products = self.db_f2.select_all_produkte()
        for product in products:
            print(product)
            product_id_f2 = product["PRODUKT_ID"]
            supplier_id = self.db_f2.get_supplier_id_with_brand_id(product["MARKE_ID"])
            product_class_id = get_new_product_class_id(self.db_f2.select_categoryid_from_productid(product_id_f2))
            product_name = product["BEZEICHNUNG"]
            sku = product["SKU"]
            discount = 0
            size_fit = 1
            if product["TYP"] == config.PRODUCT_TYP_F2[0]:
                # ToDO: gewichtsbasiert Preis umrechnen
                purchasing_price = ""
                selling_price = ""
            else:
                purchasing_price = self.db_f2.select_current_buying_price_with_product_id(product_id_f2)
                selling_price = self.db_f2.select_current_sale_price_with_product_id(product_id_f2)
            mwst = convert_mwst(float(product["UMSATZSTEUERSATZ"]))

            product_present_id = self.db_master.product_present_check_with_sku(sku, supplier_id)

            if not product_present_id:
                new_id = self.db_master.insert_product_row_only_required(supplier_id, product_class_id, product_name,
                                                                         sku, discount, size_fit,
                                                                         purchasing_price, selling_price, mwst)
                f2_master_products_connection.append([new_id, product_id_f2])
            else:
                print(product_present_id)
                # ToDo: Zuweisung einfügen
        save_f2_master_products_id_connection(f2_master_products_connection)
