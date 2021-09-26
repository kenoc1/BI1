import cx_Oracle
import config


class Lagerplatz:

    def __init__(self):
        self.con_f2 = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")
        self.con_OS = cx_Oracle.connect(user=config.DB_CON_USER_OS, password=config.DB_CON_PW_OS,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")

        self.con_combined = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                              dsn=config.DB_CON_DSN_F2,
                                              encoding="UTF-8")
        self.importliste = []

    def getLagerplaetzeF2(self):
        # nur die Verkaufsflächen
        # Lagerflächen
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select ZF2.PRODUKT_ID, LF2.REGALZEILE,LF2.REGAL_NUMMER,LF2.REGALSPALTE from LAGER_EINHEIT LF2,ZUWEISUNG_PRODUKT_LAGERPLATZ ZF2 WHERE LF2.LAGERPLATZ_ID = ZF2.LAGERPLATZ_ID AND LF2.TYP = 'Verkaufsflaeche' """)
                lagerplatzListe = cursor.fetchall()
                return lagerplatzListe

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getAnzahlVerkaufe(self, produktID):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select count(PRODUKT_ID) from STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF WHERE PRODUKT_ID = {produktID}""")
                anzahlProdukte = cursor.fetchall()
                return anzahlProdukte[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getAnzahlEinkaufe(self, produktID):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select count(PRODUKT_ID) from STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF WHERE PRODUKT_ID = {produktID}""")
                anzahlProdukte = cursor.fetchall()
                return anzahlProdukte[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getProduktIDsF2(self):
        # anpassen da die neuen ProduktIDs in der neuen Datenbank benoetigt werden
        # wir brauchen eine mapping tabeelle bei dcen produkten damit die Produkte exakt zugeordnet werden können
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select PRODUKT_ID from PRODUKT """)
                produktlist = cursor.fetchall()
                return produktlist
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def berechneMenge(self, produkt):

        einkauefe = self.getAnzahlEinkaufe(produkt)
        verkaeufe = self.getAnzahlVerkaufe(produkt)

        aktuelleMenge = einkauefe - verkaeufe
        return aktuelleMenge


# zusammebauen der eigentlichen Insert liste

object = Lagerplatz()

produktliste = object.getProduktIDsF2()
lagerplatzListe = object.getLagerplaetzeF2()

i = 0
for lagerplatz in lagerplatzListe:
    lagerplatz_ = [31, lagerplatz[0], lagerplatz[1], lagerplatz[2], lagerplatz[3],
                   object.berechneMenge(lagerplatz[0])]
    object.importliste.extend(lagerplatz_)
    i = i +1
    print(lagerplatz_)


