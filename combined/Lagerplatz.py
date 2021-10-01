import cx_Oracle
import config
import random

from combined.key_allocation_reader import read_f2_to_comb_id_allocation_to_file
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

    def getLagerplaetzeF2(self, typ):
        # gibt Liste mit ProduktID_F2 und weiteren Attributen zurück
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

    def changeProductID(self, lagerplatzliste):
        #ändere productID in der Liste gegen neue
        produktliste = read_f2_to_comb_id_allocation_to_file("products.csv")

        for lagerplatz in lagerplatzliste:
            lagerplatz[0] = search_for_id(produktliste, lagerplatz[0])

        return lagerplatzliste

    def berechneMenge(self, produkt):

        #if produkt = gewichtsbasiert dann anders berechnen
        #alte Product ID verwenden

        if(self.isgewichtsbasiert(produkt)):
            #berechnung implementieren
            #differenz * Nettogewicht /2)
            einkauefe = self.getAnzahlEinkaufe(produkt)
            verkaeufe = self.getAnzahlVerkaufe(produkt)


        einkauefe = self.getAnzahlEinkaufe(produkt)
        verkaeufe = self.getAnzahlVerkaufe(produkt)

        if einkauefe == None:
            return int(random.random() * 20 + 1)

        aktuelleMenge = einkauefe - verkaeufe
        if aktuelleMenge < 0:
            aktuelleMenge = int(random.random() * 20 + 1)
            return aktuelleMenge

    def isgewichtsbasiert(self, produkt):

        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select PRODUKT_ID from PRODUKT WHERE TYP = 'gewichtsbasiert'""")
                produktid = cursor.fetchall()
                if produktid == None:
                    return False
                else:
                    return True

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


        return True

    def insertLagerplaetze(self, lager_ID, produkt_ID, regal_reihe, ragel_spalte, akt_menge, regal_zeile):
        try:
            # add new Verkaufsfläche as column in combined DB
            with self.con_combined.cursor() as cursor:
                cursor.execute(f"""insert into LAGERPLATZ (LAGER_ID, PRODUKT_ID, REGAL_REIHE, REGAL_SPALTE, AKT_MENGE, REGAL_ZEILE)
                                    values ({lager_ID}, {produkt_ID}, {regal_reihe}, {ragel_spalte}, {akt_menge}, {regal_zeile})""")
                self.con_combined.commit()
                print("Lagerplatz hinzugefuegt:" + lager_ID, produkt_ID, regal_reihe, ragel_spalte, akt_menge, regal_zeile)

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)



lagerplatzobjekt = Lagerplatz()
lagerplatzListe_Verkaufsflaeche = lagerplatzobjekt.getLagerplaetzeF2("Verkaufsflaeche")
lagerplatzListe_Verkaufsflaeche = lagerplatzobjekt.changeProductID(lagerplatzListe_Verkaufsflaeche)
lagerplatzListe_Lagerflaeche = lagerplatzobjekt.getLagerplaetzeF2("Lagerflaeche")
lagerplatzListe_Lagerflaeche = lagerplatzobjekt.changeProductID(lagerplatzListe_Lagerflaeche)

i = 0
for lagerplatz in lagerplatzListe_Verkaufsflaeche:
    temp = [31, lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3],
                   lagerplatzobjekt.berechneMenge(lagerplatz[0])]

    lagerplatzobjekt.importliste.append(temp)
    i = i + 1
    print(temp)

i=0
for lagerplatz in lagerplatzListe_Lagerflaeche:
    temp2 = [31, lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3],
                   lagerplatzobjekt.berechneMenge(lagerplatz[0])]

    lagerplatzobjekt.importliste.append(temp2)
    i = i + 1
    print(temp2)
