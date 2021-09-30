import cx_Oracle
import config


# https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
from combined import key_allocation_saver


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
        array_for_csv = []
        # get all addresses from f2 database
        dataset = self.get_addresses()

        # loop over addresses and insert them into database
        for row in dataset:
            self.insert_addresses(row, array_for_csv)

        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(array_for_csv, "addresse.csv")



    def get_addresses(self):
        try:
            # load addresses from f2 database and return dataset
            with self.con_f2.cursor() as cursor:
                #cursor for all addresses
                # cursor.execute("""SELECT ADRESSE.STRASSE, ADRESSE.NUMMER, ORTSKENNZAHL.ORTSKENNZAHL, ORT.NAME, LAND.BEZEICHNUNG FROM ADRESSE, ORTSKENNZAHL, ORT, LAND WHERE ADRESSE.ORTSKENNZAHL_ID = ORTSKENNZAHL.ORTSKENNZAHL_ID AND ORTSKENNZAHL.ORT_ID = ORT.ORT_ID AND ORT.LAND_ID = LAND.LAND_ID""")

                #cursor for testing with first 100 addresses
                cursor.execute(
                    """SELECT ADRESSE.STRASSE, ADRESSE.NUMMER, ORTSKENNZAHL.ORTSKENNZAHL, ORT.NAME, LAND.BEZEICHNUNG, ADRESSE.ADRESS_ID FROM ADRESSE, ORTSKENNZAHL, ORT, LAND WHERE ADRESSE.ORTSKENNZAHL_ID = ORTSKENNZAHL.ORTSKENNZAHL_ID AND ORTSKENNZAHL.ORT_ID = ORT.ORT_ID AND ORT.LAND_ID = LAND.LAND_ID""")
                dataset = cursor.fetchall()
                if dataset:
                    for row in dataset:
                        print(row)
                    return dataset
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_addresses(self, row, array_for_csv):
        plz = row[2]
        ort = row[3]
        strasse = row[0]
        hnr = row[1]
        bndsland = row[4]
        alt_id = row[5]

        # test if address already exists
        if self.address_exist(row):
            on_address_id = self.get_id_of_existing_address(row)
            print("Existing: ")
            print(on_address_id)
            array_for_csv.append([on_address_id, alt_id])
            # TODO hier in csv speichern alte id und neue id
            try:
                # if address exist, alter the table ADRESSE_HERKUNFT
                with self.con_master.cursor() as cursor:
                    cursor.execute(
                    f"""INSERT INTO DATENHERKUNFT_ADRESSE (DATENHERKUNFT_ID, ADRESSE_ID) VALUES(2, {on_address_id})""")
                    # commit work
                    self.con_master.commit()
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)
        else:
            try:
                # else insert into the address table and the table DATENHERKUNFT_ADRESSE
                with self.con_master.cursor() as cursor:
                    cursor.execute(f"""INSERT INTO ADRESSE(LAND, PLZ, ORT, STRASSE, HAUSNUMMER, BUNDESLAND) VALUES('Deutschland', '{plz}', '{ort}', '{strasse}', '{hnr}', '{bndsland}')""")
                    self.con_master.commit()
                #print(f"""INSERT INTO ADRESSE(LAND, PLZ, ORT, STRASSE, HAUSNUMMER, BUNDESLAND) VALUES('Deutschland', '{plz}', '{ort}', '{strasse}', '{hnr}', '{bndsland}')""")

                with self.con_master.cursor() as cursor:
                    cursor.execute(
                        f"""SELECT ADRESSE_ID FROM ADRESSE WHERE STRASSE='{strasse}' AND PLZ='{plz}' AND ORT='{ort}' AND HAUSNUMMER='{hnr}'""")
                    line = cursor.fetchone()
                    adresse_id = line[0]
                    print(adresse_id)

                    cursor.execute(
                        f"""INSERT INTO DATENHERKUNFT_ADRESSE (DATENHERKUNFT_ID, ADRESSE_ID) VALUES(2, {adresse_id})""")
                     #commit work
                    self.con_master.commit()

                    array_for_csv.append([adresse_id, alt_id])
                    #adresse id csv mit alt_id und neu id erst neu dann alt
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)


    def address_exist(self, row):
        #print("Methode address_exist")
        plz = row[2]
        ort = row[3]
        strasse = row[0]
        hnr = row[1]
        try:
            with self.con_master.cursor() as cursor:
                sql = f"""SELECT COUNT(ADRESSE.ADRESSE_ID) FROM ADRESSE WHERE ADRESSE.STRASSE='{strasse}' AND ADRESSE.PLZ='{plz}' AND ADRESSE.ORT='{ort}' AND ADRESSE.HAUSNUMMER='{hnr}'"""
                cursor.execute(sql)
                result = cursor.fetchone()
                #print(result)
                if result[0] > 0:
                    return True
        except cx_Oracle.Error as error:
            print("address_exist_error")
            print('Error occurred:')
            print(error)
        return False

    def get_id_of_existing_address(self, row):
        print("Methode get_id_of_existing_address")
        plz = row[2]
        ort = row[3]
        strasse = row[0]
        hnr = row[1]
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute(f"""SELECT ADRESSE_ID FROM ADRESSE WHERE STRASSE='{strasse}' AND PLZ='{plz}' AND ORT='{ort}' AND HAUSNUMMER='{hnr}'""")
                result = cursor.fetchone()
                return result[0]
        except cx_Oracle.Error as error:
            print("get_id_of_existing_address_error")
            print('Error occurred:')
            print(error)
        return 0

# Onjekt erstellen
tst = AddressMerge()
# Methodenaufruf, bitte nur mit get_addresses testen auf keinen Fall start ausführen!
tst.start()
