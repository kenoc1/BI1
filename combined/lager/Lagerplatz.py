import cx_Oracle

import config
from combined.key_allocation_reader import read_f2_to_comb_id_allocation_from_file
from util import search_for_id


class Lagerplatz:

    def __init__(self):
        self.con_f2 = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")

        self.con_combined = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                              dsn=config.DB_CON_DSN_F2,
                                              encoding="UTF-8")
        self.importliste = []

    # -----------------datenbankaufrufe-----------------
    def get_lagerplaetze_f2(self, typ):
        # gibt Liste mit ProduktID_F2 und weiteren Attributen zurueck
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select ZF2.PRODUKT_ID, LF2.REGALZEILE,LF2.REGAL_NUMMER,LF2.REGALSPALTE from LAGER_EINHEIT LF2,ZUWEISUNG_PRODUKT_LAGERPLATZ ZF2 WHERE LF2.LAGERPLATZ_ID = ZF2.LAGERPLATZ_ID AND LF2.TYP = '{typ}'""")
                lagerplatz_liste = cursor.fetchall()
                return lagerplatz_liste
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_anzahl_verkaeufe(self, product_id_f2):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select sum(anzahl_Produkte) from STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF WHERE PRODUKT_ID = {product_id_f2}""")
                anzahl_verkaufe = cursor.fetchall()
                return anzahl_verkaufe[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_anzahl_einkaeufe(self, product_id):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select sum(anzahl_Produkte) from STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF WHERE PRODUKT_ID = {product_id}""")
                anzahl_einkaufe = cursor.fetchall()
                return anzahl_einkaufe[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_nettogewicht(self, produkt):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select NETTOGEWICHT from PRODUKT WHERE PRODUKT_ID = {produkt}""")
                nettogewicht = cursor.fetchall()
                return nettogewicht[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def is_gewichtsbasiert(self, produkt) -> bool:
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select TYP from PRODUKT WHERE  PRODUKT_ID = {produkt}""")
                temp = cursor.fetchall()
                typ = temp[0][0]
                if typ == "stueckbasiert":
                    return False
                else:
                    return True
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def exists_lagerplatz(self, lager_id, product_id) -> bool:
        try:
            with self.con_combined.cursor() as cursor:
                cursor.execute(
                    f"""select LAGERPLATZ_ID from LAGERPLATZ WHERE PRODUKT_ID = {product_id} AND LAGER_ID = {lager_id}""")
                lagerplatz_id = cursor.fetchall()
                if lagerplatz_id is None:
                    return False
                else:
                    return True
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_lager_id(self, anzahl_laderampen):
        # holt die LagerID anhand der Laderampen
        # Verkaufsflächen = 1
        # Lagerflächen = 2
        try:
            with self.con_combined.cursor() as cursor:
                cursor.execute(
                    f"""select LAGER_ID from LAGER WHERE LADERAMPEN = {anzahl_laderampen} AND DATENHERKUNFT_ID = 2""")
                lager_id = cursor.fetchall()
                return lager_id[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    # ----------------berechnungen--------------------
    def get_new_product_id(self):
        # aendere productID in der Liste gegen neue
        produktliste = read_f2_to_comb_id_allocation_from_file(config.PRODUCTS_CON_FILE_NAME)
        for lagerplatz in self.importliste:
            lagerplatz[1] = search_for_id(produktliste, lagerplatz[1])

    # ------insert Lagerplätze-------

    def insert_lagerplaetze(self, lager_id, produkt_id, regal_zeile, regal_nummer, regal_spalte, akt_menge):

        if not self.exists_lagerplatz(lager_id, produkt_id):
            try:
                # add new Verkaufsflaeche as column in combined DB
                with self.con_combined.cursor() as cursor:
                    cursor.execute(f"""insert into LAGERPLATZ (LAGER_ID, PRODUKT_ID, REGAL_REIHE, REGAL_SPALTE, AKT_MENGE, REGAL_ZEILE)
                                        values ({lager_id}, {produkt_id}, {regal_nummer}, {regal_spalte}, {akt_menge}, {regal_zeile})""")
                    self.con_combined.commit()
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)
        else:
            print("Lagerplatz existiert schon")


# ------main-------
if __name__ == "__main__":
    lagerplatzobjekt = Lagerplatz()
    f2_verkaufsflaechen = lagerplatzobjekt.get_lagerplaetze_f2("Verkaufsflaeche")
    f2_lagerflaechen = lagerplatzobjekt.get_lagerplaetze_f2("Lagerflaeche")

    zaehler = 0
    for lagerplatz in f2_verkaufsflaechen:
        temp = [lagerplatzobjekt.get_lager_id(1), lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3], -100000]
        lagerplatzobjekt.importliste.append(temp)
        zaehler = zaehler + 1

    zaehler = 0
    for lagerplatz in f2_lagerflaechen:
        temp2 = [lagerplatzobjekt.get_lager_id(2), lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3], -100000]
        lagerplatzobjekt.importliste.append(temp2)
        zaehler = zaehler + 1

    print("Liste erstellt")
    lagerplatzobjekt.get_new_product_id()

    for lagerplatz in lagerplatzobjekt.importliste:
        lagerplatzobjekt.insert_lagerplaetze(lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3], lagerplatz[4],
                                             lagerplatz[5])
        print("insert:", lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3], lagerplatz[4],
              lagerplatz[5])
