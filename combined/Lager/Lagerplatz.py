import cx_Oracle
import config
import random

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
    def getLagerplaetzeF2(self, typ):
        # gibt Liste mit ProduktID_F2 und weiteren Attributen zurueck
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select ZF2.PRODUKT_ID, LF2.REGALZEILE,LF2.REGAL_NUMMER,LF2.REGALSPALTE from LAGER_EINHEIT LF2,ZUWEISUNG_PRODUKT_LAGERPLATZ ZF2 WHERE LF2.LAGERPLATZ_ID = ZF2.LAGERPLATZ_ID AND LF2.TYP = '{typ}'""")
                lagerplatzListe = cursor.fetchall()
                return lagerplatzListe

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getAnzahlVerkaufe(self, produktID_F2):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select sum(anzahl_Produkte) from STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF WHERE PRODUKT_ID = {produktID_F2}""")
                anzahlVerkaufe = cursor.fetchall()
                return anzahlVerkaufe[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getAnzahlEinkaufe(self, produktID):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select sum(anzahl_Produkte) from STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF WHERE PRODUKT_ID = {produktID}""")
                anzahlEinkaufe = cursor.fetchall()
                return anzahlEinkaufe[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getNettogewicht(self, produkt):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select NETTOGEWICHT from PRODUKT WHERE PRODUKT_ID = {produkt}""")
                nettogewicht = cursor.fetchall()
                return nettogewicht[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def isgewichtsbasiert(self, produkt):

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

    def LagerplatzExist(self, lager_ID, produkt_ID):
        try:
            with self.con_combined.cursor() as cursor:
                cursor.execute(
                    f"""select LAGERPLATZ_ID from LAGERPLATZ WHERE PRODUKT_ID = {produkt_ID} AND LAGER_ID = {lager_ID}""")
                lagerplatzID = cursor.fetchall()
                if lagerplatzID == None:
                    return False
                else:
                    return True

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getLagerID(self, anzLaderampen):
        # holt die LagerID anhand der Laderampen
        # Verkaufsflächen = 1
        # Lagerflächen = 2
        try:
            with self.con_combined.cursor() as cursor:
                cursor.execute(
                    f"""select LAGER_ID from LAGER WHERE LADERAMPEN = {anzLaderampen} AND DATENHERKUNFT_ID = 2""")
                lagerID = cursor.fetchall()
                return lagerID[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    # ----------------berechnungen--------------------
    def changeProductID(self):
        # aendere productID in der Liste gegen neue
        produktliste = read_f2_to_comb_id_allocation_from_file(config.PRODUCTS_CON_FILE_NAME)

        for lagerplatz in self.importliste:
            lagerplatz[1] = search_for_id(produktliste, lagerplatz[1])

    def berechneMenge(self, produkt):
        # alte Product ID verwenden
        einkauefe = self.getAnzahlEinkaufe(produkt)
        verkaeufe = self.getAnzahlVerkaufe(produkt)

        if (self.isgewichtsbasiert(produkt)):
            return self.berechneGewichtsbasierteMenge(produkt, einkauefe, verkaeufe)

        else:
            return self.berechneStueckzahlbasierteMenge(einkauefe, verkaeufe)

    def berechneGewichtsbasierteMenge(self, produkt, einkauefe, verkaeufe):

        if einkauefe == None:
            return round(float(random.random() * 20 + 1), 2)

        nettogewicht = self.getNettogewicht(produkt)
        menge = (einkauefe - verkaeufe) * nettogewicht / 2

        if menge < 0:
            menge = float(random.random() * 20 + 1)
            return round(menge, 2)
        else:
            menge = (einkauefe - verkaeufe) * nettogewicht / 2
            return round(menge, 2)

    def berechneStueckzahlbasierteMenge(self, einkauefe, verkaeufe):

        menge = einkauefe - verkaeufe
        if einkauefe == None:
            return int(random.random() * 20 + 1)
        elif menge < 0:
            menge = int(random.random() * 20 + 1)
            return menge
        else:
            return menge

    # ------insert Lagerplätze-------

    def insertLagerplaetze(self, lager_ID, produkt_ID, regal_zeile, regal_nummer, regal_spalte, akt_menge):

        if (self.LagerplatzExist(lager_ID, produkt_ID) == False):
            try:
                # add new Verkaufsflaeche as column in combined DB
                with self.con_combined.cursor() as cursor:
                    cursor.execute(f"""insert into LAGERPLATZ (LAGER_ID, PRODUKT_ID, REGAL_REIHE, REGAL_SPALTE, AKT_MENGE, REGAL_ZEILE)
                                        values ({lager_ID}, {produkt_ID}, {regal_nummer}, {regal_spalte}, {akt_menge}, {regal_zeile})""")
                    self.con_combined.commit()

            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)
        else:
            print("Lagerplatz existiert schon")


# ------main-------
if __name__ == "__main__":
    lagerplatzobjekt = Lagerplatz()
    lagerplatzListe_Verkaufsflaeche = lagerplatzobjekt.getLagerplaetzeF2("Verkaufsflaeche")
    lagerplatzListe_Lagerflaeche = lagerplatzobjekt.getLagerplaetzeF2("Lagerflaeche")

    zaehler = 0
    for lagerplatz in lagerplatzListe_Verkaufsflaeche:
        temp = [lagerplatzobjekt.getLagerID(1), lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3],
                lagerplatzobjekt.berechneMenge(lagerplatz[0])]

        lagerplatzobjekt.importliste.append(temp)
        zaehler = zaehler + 1

    zaehler = 0
    for lagerplatz in lagerplatzListe_Lagerflaeche:
        temp2 = [lagerplatzobjekt.getLagerID(2), lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3],
                 lagerplatzobjekt.berechneMenge(lagerplatz[0])]
        lagerplatzobjekt.importliste.append(temp2)
        zaehler = zaehler + 1

    print("Liste erstellt")
    lagerplatzobjekt.changeProductID()

    for lagerplatz in lagerplatzobjekt.importliste:
        lagerplatzobjekt.insertLagerplaetze(lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3], lagerplatz[4],
                                            lagerplatz[5])
        print("insert:", lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3], lagerplatz[4],
              lagerplatz[5])
