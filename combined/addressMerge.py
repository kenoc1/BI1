import cx_Oracle

import config

class TEST:
    def __init__(self):
        #DB-Verbindung zu F2
        self.con_f2 = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")
        print("Database version:", self.con_f2.version)

    def get_adresses(self):
        try:
            #laden aller Adressen aus F2 mit ID<100
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select * from ADRESSE WHERE ADRESS_ID < 100""")
                #https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
                row = cursor.fetchall()
                if row:
                    for r in row:
                        print(r)
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


#Onjekt erstellen
tst = TEST()
#Methodenaufruf
tst.get_adresses()

