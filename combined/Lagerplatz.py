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


    def getLagerplaetzeF2(self):

        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select LAGERPLATZ_ID,REGALZEILE,REGAL_NUMMER,REGALSPALTE from LAGER_EINHEIT """)
                datalist_F2 = cursor.fetchall()
                print(datalist_F2)

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)



    def getAnzahlVerkaufe(self, produktID):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"""select count(PRODUKT_ID) from STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF WHERE PRODUKT_ID = {produktID}""")
                anzahlProdukte = cursor.fetchall()
                return anzahlProdukte

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


    def getAnzahlEinkaufe(self, produktID):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"""select count(PRODUKT_ID) from STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF WHERE PRODUKT_ID = {produktID}""")
                anzahlProdukte = cursor.fetchall()
                return anzahlProdukte

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)













id= 2001

object = Lagerplatz()

print(object.getAnzahlEinkaufe(id))
print(object.getAnzahlVerkaufe(id))

