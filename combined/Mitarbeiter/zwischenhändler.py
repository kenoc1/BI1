import cx_Oracle
import config
from combined.ImportKunden.AnredenFinder import AnredenFinder
from combined.key_allocation_reader import read_f2_to_comb_id_allocation_to_file
from util import search_for_id


class Zwischenhaendler_Merge:
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

    def findAdressId(self, alte_adress_id):
        test = read_f2_to_comb_id_allocation_to_file("addresse.csv")
        neueId = search_for_id(test, alte_adress_id)
        return neueId

    def getLieferantF2(self):
        try:
            # get alle Lieferant in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Lieferant_ID, Name, Email, Name_Ansprechpartner, Vorname_Ansprechpartner, Adress_ID
                        from Lieferant""")
                LieferantList = cursor.fetchall()
                return LieferantList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getEinkaufF2(self):
        try:
            # get alle Einkäufe in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Einkauf_ID, Einkaufsdatum, Lieferant_ID, Mitarbeiter_ID
                        from Einkauf""")
                EinkaufList = cursor.fetchall()
                return EinkaufList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getEinkaufstuckzahlProdukteF2(self):
        try:
            # get alle Einkäufe, die stückzahlbassierte Produkte hat in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Einkauf_ID, Produkt_Id, Anzahl_Produkte
                        from STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF""")
                EinkaufstuckzahlProdukteList = cursor.fetchall()
                return EinkaufstuckzahlProdukteList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getEinkaufgewichtProdukteF2(self):
        try:
            # get alle Einkäufe, die gewichtbassierte Produkte hat in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Einkauf_ID, Produkt_Id, Gewicht
                        from GEWICHTBASIERTES_PRODUKT_IM_EINKAUF""")
                EinkaufgewichtProdukteList = cursor.fetchall()
                return EinkaufgewichtProdukteList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insertZwischenhaendler(self):
        LieferantList = object.getLieferantF2()
        try:
            with self.con_master.cursor() as cursor:
                for Lieferant in LieferantList:
                    name = Lieferant[1]
                    email = Lieferant[2]
                    adressId = self.findAdressId(Lieferant[5])
                    nameAnsprechpartner = Lieferant[3]
                    nachnameAnsprechpartner = Lieferant[4]
                    # print (name, email, adressId,nameAnsprechpartner, nachnameAnsprechpartner)
                    cursor.execute(f"""INSERT INTO Zwischenhaendler(Name, Email, Name_Ansprechpartner, Vorname_Ansprechpartner, Adress_ID)
                                            VALUE ({name}, {email}, , {nameAnsprechpartner}, {nachnameAnsprechpartner} {adressId})""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insertEinkau(self):
        EinkaufList = object.getEinkauf()
        try:
            with self.con_master.cursor() as cursor:
                for Einkauf in EinkaufList:
                    einkaufdatum = Einkauf[1]
                    # Frage             lieferantId = Einkauf[2]
                    # Frage             MitarbeiterId = Einkauf[3]
                    # print
                    cursor.execute(f"""INSERT INTO Einkauf(Einkaufsdatum)
                                            VALUE ({einkaufdatum}""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


if __name__ == "__main__":
    # Frage Einkauf_Produkt

    object = Zwischenhaendler_Merge
