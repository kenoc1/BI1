import cx_Oracle
import config

class Lager_Merge:
    def __init__(self):
        # DB-Verbindung zu F2
        self.con_f2 = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")
        print("Database version:", self.con_f2.version)

        # DB-Verbindung zum neuen Schema Combined
        self.con_master = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                            dsn=config.DB_CON_DSN_COMBINED, encoding="UTF-8")
        print("Database version:", self.con_master.version)

    def insertNewAdress(self):
        # add new Adress for F2 Lager
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute("""insert into Adresse (Land, PLZ, Ort, STRASSE, HAUSNUMMER)
                                    VALUES (Pazifik, 32965, IBS_Insel, Babastrasse, 1)""")
                self.con_master.commit()
            # add new Verkaufsfläche as column in combined DB
            with self.con_master.cursor() as cursor:
                cursor.execute("""insert into LAGER (ADRESSE_ID, LADERAMPEN, LAGERART)
                                    values ( 1, innen)""")
                self.con_master.commit()
            # add new Lagerflaeche as column in combined DB
            with self.con_master.cursor() as cursor:
                cursor.execute("""insert into LAGER (ADRESSE_ID, LADERAMPEN, LAGERART)
                                    values (3976, 1, innen)""")
                self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def addRegalzeile(self):
        try:
            # load new attribiut "Regalzeile" from combined DB
            with self.con_master.cursor() as cursor:
                cursor.execute("""Alter Table LAGERPLATZ add Regalzeile Number""")
                self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def addDiscription(self):
        try:
            # add a comment in Lagerplatz table
            with self.con_master.cursor() as cursor:
                cursor.execute("""comment on table LAGERPLATZ is Alle Einheiten sind in inch und die Regalbreite ist=12 , Regalriefe ist=20 und Regalhöhe ist=16 """)
                self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getLagerEinheitenVerkaufsflaeche(self):
        try:
            # get all verkaufsfläsche_Lager_Einheiten im Lager f2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select REGAL_NUMMER, REGALSPALTE, REGALZEILE from LAGER_EINHEIT where TYP='Verkaufsfläche'""")
                lagerEinheitenListeVerkaufsflaeche = cursor.fetchall()
                print (lagerEinheitenListeVerkaufsflaeche)
                return lagerEinheitenListeVerkaufsflaeche
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getLagerEinheitenLagerflaeche(self):
        try:
            # get all Lagerfläsche_Lager_Einheiten im Lager f2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select REGAL_NUMMER, REGALSPALTE, REGALZEILE from LAGER_EINHEIT where TYP='Lagerfläche'""")
                lagerEinheitenListeLagerflaeche = cursor.fetchall();
                print(lagerEinheitenListeLagerflaeche)
                return lagerEinheitenListeLagerflaeche
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getAnzahlStueckzahlProdukt(self):
        try:
            # get amount all stuckzahlbasierte Produkte from Lager f2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF.PRODUKT_ID, sum(STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF.anzahl_Produkte)
                        from EINKAUF, STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF
                        where EINKAUF.EINKAUFS_ID = STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF.EINKAUFS_ID
                        group by STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF.PRODUKT_ID.PRODUKT_ID
                        order by STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF.PRODUKT_ID.PRODUKT_ID""")
                stueckzahlListeEinkauf = cursor.fetchall()
                cursor.execute(
                    """select STUECKZAHLBASIERTES_PRODUKT_IM_VerKAUF.PRODUKT_ID, sum(STUECKZAHLBASIERTES_PRODUKT_IM_verKAUF.anzahl_Produkte)
                        from verKAUF, STUECKZAHLBASIERTES_PRODUKT_IM_verKAUF
                        where verKAUF.verKAUFS_ID = STUECKZAHLBASIERTES_PRODUKT_IM_verKAUF.verKAUFS_ID
                        group by STUECKZAHLBASIERTES_PRODUKT_IM_verKAUF.PRODUKT_ID
                        order by STUECKZAHLBASIERTES_PRODUKT_IM_verKAUF.PRODUKT_ID""")
                stueckzahlListeVerkauf = cursor.fetchall()
                for i in stueckzahlListeVerkauf:
                    if stueckzahlListeEinkauf[i] == stueckzahlListeVerkauf[i]:
                        print (stueckzahlListeEinkauf[i][1] - stueckzahlListeVerkauf[i][1])
                        return stueckzahlListeEinkauf - stueckzahlListeVerkauf
                        i = i + 1
                    else:
                        return stueckzahlListeEinkauf
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getGewichtGewichtProdukt(self):
        try:
            # get amount all gewichtbasierte Produkte from Lager f2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select GEWICHTBASIERTES_PRODUKT_IM_EINKAUF.PRODUKT_ID, sum(GEWICHTBASIERTES_PRODUKT_IM_EINKAUF.GEWICHT)
                        from EINKAUF, GEWICHTBASIERTES_PRODUKT_IM_EINKAUF
                        where EINKAUF.EINKAUFS_ID = GEWICHTBASIERTES_PRODUKT_IM_EINKAUF.EINKAUFS_ID
                        group by GEWICHTBASIERTES_PRODUKT_IM_EINKAUF.PRODUKT_ID
                        order by GEWICHTBASIERTES_PRODUKT_IM_EINKAUF.PRODUKT_ID""")
                gewichtbasierteListeEinkauf = cursor.fetchall()
                cursor.execute(
                    """select GEWICHTBASIERTES_PRODUKT_IM_verKAUF.PRODUKT_ID, sum(GEWICHTBASIERTES_PRODUKT_IM_verKAUF.GEWICHT)
                        from VERKAUF, GEWICHTBASIERTES_PRODUKT_IM_verKAUF
                        where VERKAUF.VERKAUFS_ID = GEWICHTBASIERTES_PRODUKT_IM_verKAUF.verKAUFS_ID
                        group by GEWICHTBASIERTES_PRODUKT_IM_verKAUF.PRODUKT_ID
                        order by GEWICHTBASIERTES_PRODUKT_IM_verKAUF.PRODUKT_ID""")
                gewichtbasierteListeVerkauf = cursor.fetchall()
                for i in gewichtbasierteListeEinkauf :
                    if gewichtbasierteListeEinkauf[i] == gewichtbasierteListeVerkauf [i]:
                        print (gewichtbasierteListeEinkauf [i][1] - gewichtbasierteListeVerkauf [i][1])
                        return gewichtbasierteListeEinkauf - gewichtbasierteListeVerkauf
                    else:
                        return gewichtbasierteListeEinkauf
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
if __name__ == "__main__":
    object = Lager_Merge()
    # templiste = object.insertNewAdress()
    # templiste = object.addRegalzeile()
    # templiste = object.addDiscription()
    templiste = object.getLagerEinheitenLagerflaeche()
    templiste = object.getLagerEinheitenVerkaufsflaeche()
    templiste = object.getAnzahlStueckzahlProdukt()
    templiste = object.getGewichtGewichtProdukt()

    print(templiste)
