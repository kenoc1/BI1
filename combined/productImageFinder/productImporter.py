# Alle Produkte aus der Datenbank holen: ID, NAME, MARKE


# Datenbankverbindung herstellen
import sys

import cx_Oracle

import config
from combined import key_allocation_saver


class ProductImporter:
    def __init__(self):
        self.con_master = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                            dsn=config.DB_CON_DSN_COMBINED, encoding="UTF-8")
        print("Database version:", self.con_master.version)

    def getProducts(self):

        with self.con_master.cursor() as cursor:
            cursor.execute(
                """SELECT PRODUKT.PRODUKT_ID, PRODUKT.PROUKT_NAME, MARKE.BEZEICHNUNG FROM PRODUKT, MARKE WHERE PRODUKT.MARKE_ID = MARKE.MARKE_ID AND PRODUKT.PRODUKTBILD_LINK IS NULL""")
            dataset = cursor.fetchall()
            if (dataset):
                return dataset
            else:
                print('Error occurred:')

