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

#-----------------datenbankaufrufe-----------------
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

#----------------berechnungen--------------------
    def changeProductID(self):
        #ändere productID in der Liste gegen neue
        #ToDO: Die Methode testen wenn Keno die csv bereit hat
        produktliste = read_f2_to_comb_id_allocation_to_file("products.csv")

        for lagerplatz in self.importliste:
            lagerplatz[0] = search_for_id(produktliste, lagerplatz[0])


    def berechneMenge(self, produkt):
        # alte Product ID verwenden
        einkauefe = self.getAnzahlEinkaufe(produkt)
        verkaeufe = self.getAnzahlVerkaufe(produkt)

        if(self.isgewichtsbasiert(produkt)):
            return self.berechneGewichtsbasierteMenge(produkt, einkauefe, verkaeufe)

        else:
            return self.berechneStueckzahlbasierteMenge(einkauefe, verkaeufe)


    def berechneGewichtsbasierteMenge(self, produkt, einkauefe,verkaeufe):

        if einkauefe == None:
            return float(random.random() * 20 + 1)

        nettogewicht = self.getNettogewicht(produkt)
        menge = (einkauefe - verkaeufe) * nettogewicht / 2

        if menge < 0:
            menge = float(random.random() * 20 + 1)
            return menge
        else:
            menge = (einkauefe - verkaeufe) * nettogewicht / 2
            return menge

    def berechneStueckzahlbasierteMenge(self, einkauefe, verkaeufe):

        menge = einkauefe - verkaeufe
        if einkauefe == None:
            return int(random.random() * 20 + 1)
        elif menge < 0:
            menge = int(random.random() * 20 + 1)
            return menge
        else:
            return menge


#------insert Lagerplätze-------

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




#------main-------
#ToDo:lager ID anpassen
lagerplatzobjekt = Lagerplatz()
lagerplatzListe_Verkaufsflaeche = lagerplatzobjekt.getLagerplaetzeF2("Verkaufsflaeche")
lagerplatzListe_Lagerflaeche = lagerplatzobjekt.getLagerplaetzeF2("Lagerflaeche")

zaehler = 0
for lagerplatz in lagerplatzListe_Verkaufsflaeche:
    temp = [31, lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3],
                   lagerplatzobjekt.berechneMenge(lagerplatz[0])]

    lagerplatzobjekt.importliste.append(temp)
    zaehler = zaehler + 1
    print(temp)

zaehler = 0
for lagerplatz in lagerplatzListe_Lagerflaeche:
    temp2 = [32, lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3],
                   lagerplatzobjekt.berechneMenge(lagerplatz[0])]

    lagerplatzobjekt.importliste.append(temp2)
    zaehler = zaehler + 1
    print(temp2)

#lagerplatzobjekt.changeProductID()