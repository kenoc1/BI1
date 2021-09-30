import cx_Oracle
import config


class Lager:
    def __init__(self):
        self.con_f2 = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")

        self.con_combined = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                              dsn=config.DB_CON_DSN_F2,
                                              encoding="UTF-8")
        print("Database version:", self.con_combined.version)

    def getDummyAdresse(self):

        try:
            with self.con_combined.cursor() as cursor:
                cursor.execute(
                    f"""select ADRESSE_ID from ADRESSE WHERE LAND = 'nicht vorhanden'""")
                dummyAdressID = cursor.fetchall()
                return dummyAdressID[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getHerkunftsID(self):
        try:
            with self.con_combined.cursor() as cursor:
                cursor.execute(
                    f"""select DATENHERKUNFT_ID from DATENHERKUNFT WHERE BEZEICHNUNG = 'Filiale 2'""")
                datenherkunftsID = cursor.fetchall()
                return datenherkunftsID[0][0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insertLager_Verkaufsflaechen(self,dummyAdressID, datenherkunftsID):
        try:
            # add new Verkaufsfläche as column in combined DB
            with self.con_combined.cursor() as cursor:
                cursor.execute(f"""insert into LAGER (ADRESSE_ID, LADERAMPEN, LAGERART, DATENHERKUNFT_ID)
                                    values ({dummyAdressID}, 1, 'innen', {datenherkunftsID}) """)
                self.con_combined.commit()
                print("Lager hinzugefuegt"+ dummyAdressID, 1, 'innen', datenherkunftsID)
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insertLager_Lagerflaechen(self,dummyAdressID, datenherkunftsID):
        try:
            with self.con_combined.cursor() as cursor:
                cursor.execute(f"""insert into LAGER (ADRESSE_ID, LADERAMPEN, LAGERART, DATENHERKUNFT_ID)
                                    values ({dummyAdressID}, 1, 'innen', {datenherkunftsID}) """)
                self.con_combined.commit()
                print("Lager hinzugefuegt"+ dummyAdressID, 1, 'innen', datenherkunftsID)
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


lagerobjekt = Lager()
dummyAdressID = lagerobjekt.getDummyAdresse()
datenherkunftsID = lagerobjekt.getHerkunftsID()
#lagerobjekt.insertLager_Verkaufsflaechen(dummyAdressID, datenherkunftsID)
#lagerobjekt.insertLager_Lagerflaechen(dummyAdressID, datenherkunftsID)

