import cx_Oracle
import config


# https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/

class AddressMerge:
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

    def start(self):
        # add Bundesland as column in combined database
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute("""ALTER TABLE ADRESSE ADD Bundesland varchar(100)""")
                self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

        # get all addresses from f2 database
        dataset = self.get_addresses()

        # loop over addresses and insert them into database
        for row in dataset:
            self.insert_address(row)



    def get_addresses(self):
        try:
            # load addresses from f2 database and return dataset
            with self.con_f2.cursor() as cursor:
                #cursor for all addresses
                # cursor.execute("""SELECT ADRESSE.STRASSE, ADRESSE.NUMMER, ORTSKENNZAHL.ORTSKENNZAHL, ORT.NAME, LAND.BEZEICHNUNG FROM ADRESSE, ORTSKENNZAHL, ORT, LAND WHERE ADRESSE.ORTSKENNZAHL_ID = ORTSKENNZAHL.ORTSKENNZAHL_ID AND ORTSKENNZAHL.ORT_ID = ORT.ORT_ID AND ORT.LAND_ID = LAND.LAND_ID""")

                #cursor for testing with first 100 addresses
                cursor.execute(
                    """SELECT ADRESSE.STRASSE, ADRESSE.NUMMER, ORTSKENNZAHL.ORTSKENNZAHL, ORT.NAME, LAND.BEZEICHNUNG FROM ADRESSE, ORTSKENNZAHL, ORT, LAND WHERE ADRESSE.ORTSKENNZAHL_ID = ORTSKENNZAHL.ORTSKENNZAHL_ID AND ORTSKENNZAHL.ORT_ID = ORT.ORT_ID AND ORT.LAND_ID = LAND.LAND_ID AND ADRESS_ID < 100""")
                dataset = cursor.fetchall()
                if dataset:
                    #for row in dataset:
                    #    print(row)
                    return dataset
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_addresses(self, row):
        # test if address already exists
        if self.address_exist(row):
            address_id = self.get_id_of_existing_address(row)
            try:
                # if address exist, alter the table ADRESSE_HERKUNFT
                with self.con_master.cursor() as cursor:
                    cursor.execute(
                    f"""UPDATE ADRESSE_HERKUNFT SET HERKUNFT_ID= (SELECT HERKUNFT_ID FROM HERKUNFT WHERE BEZEICHNUNG='Beide') WHERE ADRESSE_ID = {address_id}""")
                    # commit work
                    self.con_master.commit()
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)
        else:
            plz = row[2]
            ort = row[3]
            strasse = row[0]
            hnr = row[1]
            bndsland = row[4]

            try:
                # else insert into the address table and the table ADRESSE_HERKUNFT
                with self.con_master.cursor() as cursor:
                    cursor.execute(f"""INSERT INTO ADRESSE(LAND, PLZ, ORT, STRASSE, HAUSNUMMER, BUNDESLAND) VALUES('Deutschland', {plz}, {ort}, {strasse}, {hnr}, {bndsland})""")
                    self.con_master.commit()

                with self.con_master.cursor() as cursor:
                    cursor.excecute(
                        f"""SELECT ADRESSE_ID FROM ADRESSE WHERE STRASSE={strasse} AND PLZ={plz} AND ORT={ort} AND HAUSNUMMER={hnr}""")
                    adresse_id = cursor.fetchone()[0]
                    cursor.excecute(
                        """SELECT HERKUNFT_ID FROM HERKUNFT WHERE Bezeichnung='Filiale 2'""")
                    herkunft_id = cursor.fetchone()[0]
                    cursor.execute(
                        f"""INSERT INTO ADRESSE_HERKUNFT(HERKUNFT_ID, ADRESSE_ID) VALUES({herkunft_id}, {adresse_id})""")
                    # commit work
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)


    def address_exist(self, row):
        plz = row[2]
        ort = row[3]
        strasse = row[0]
        hnr = row[1]
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute(f"""SELECT COUNT(ADRESSE_ID) FROM ADRESSE WHERE STRASSE={strasse} AND PLZ={plz} AND ORT={ort} AND HAUSNUMMER={hnr}""")
                result = cursor.fetchone()
                if result[0] > 0:
                    return True
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
        return False

    def get_id_of_existing_address(self, row):
        plz = row[2]
        ort = row[3]
        strasse = row[0]
        hnr = row[1]
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute(f"""SELECT ADRESSE_ID FROM ADRESSE WHERE STRASSE={strasse} AND PLZ={plz} AND ORT={ort} AND HAUSNUMMER={hnr}""")
                result = cursor.fetchone()
                return result[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
        return 0

# Onjekt erstellen
tst = AddressMerge()
# Methodenaufruf, bitte nur mit get_addresses testen auf keinen Fall start ausführen!
tst.get_addresses()
