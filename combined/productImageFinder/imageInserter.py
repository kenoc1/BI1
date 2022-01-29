import cx_Oracle

import config


class ImageInserter:
    def __init__(self):
        self.con_master = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                            dsn=config.DB_CON_DSN_COMBINED, encoding="UTF-8")
        print("Database version:", self.con_master.version)

    def insertImageUrls(self, product):
        print("In die Datenbank schreiben...")
        with self.con_master.cursor() as cursor:
            try:
                cursor.execute(
                    f"""UPDATE PRODUKT set PRODUKTBILD_LINK = '{product[2]}' WHERE PRODUKT_ID ='{product[1]}'""")
                self.con_master.commit()
                print("In der Datenbank ergänzt")
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)
