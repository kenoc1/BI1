import cx_Oracle
import config
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
        # create array for csv
        array_for_csv = []

        # get all addresses from f2 database
        dataset = self.get_addresses()

        # loop over addresses and insert them into database
        for row in dataset:
            self.insert_addresses(row, array_for_csv)

        # create csv for allocation
        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(array_for_csv, "adresse.csv")

    # load addresses from f2 database and return dataset
    def get_addresses(self):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """SELECT ADRESSE.STRASSE, ADRESSE.NUMMER, ORTSKENNZAHL.ORTSKENNZAHL, ORT.NAME, LAND.BEZEICHNUNG, ADRESSE.ADRESS_ID FROM ADRESSE, ORTSKENNZAHL, ORT, LAND WHERE ADRESSE.ORTSKENNZAHL_ID = ORTSKENNZAHL.ORTSKENNZAHL_ID AND ORTSKENNZAHL.ORT_ID = ORT.ORT_ID AND ORT.LAND_ID = LAND.LAND_ID""")
                dataset = cursor.fetchall()
                if dataset:
                    return dataset
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    # insert one address to database
    def insert_addresses(self, row, array_for_csv):
        # get attributes
        street = row[0]
        number = row[1]
        local_area_code = row[2]
        city = row[3]
        area = row[4]
        old_id = row[5]

        # test if address already exists
        if self.address_exist(row):
            #get id of existing address
            combined_address_id = self.get_id_of_existing_address(row)

            # add old and new id to array
            array_for_csv.append([combined_address_id, old_id])

            try:
                # alter the table DATENHERKUNG_ADRESSE
                with self.con_master.cursor() as cursor:
                    cursor.execute(
                    f"""INSERT INTO DATENHERKUNFT_ADRESSE (DATENHERKUNFT_ID, ADRESSE_ID) VALUES(2, {combined_address_id})""")
                    self.con_master.commit()
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)
        else:
            try:
                # else insert the new address
                with self.con_master.cursor() as cursor:
                    cursor.execute(f"""INSERT INTO ADRESSE(LAND, PLZ, ORT, STRASSE, HAUSNUMMER, BUNDESLAND) VALUES('Deutschland', '{local_area_code}', '{city}', '{street}', '{number}', '{area}')""")
                    self.con_master.commit()

                with self.con_master.cursor() as cursor:
                    # get new address_id
                    cursor.execute(
                        f"""SELECT ADRESSE_ID FROM ADRESSE WHERE STRASSE='{street}' AND PLZ='{local_area_code}' AND ORT='{city}' AND HAUSNUMMER='{number}'""")
                    address_id = cursor.fetchone()[0]

                    # insert DATENHERKUNFT
                    cursor.execute(
                        f"""INSERT INTO DATENHERKUNFT_ADRESSE (DATENHERKUNFT_ID, ADRESSE_ID) VALUES(2, {address_id})""")

                    self.con_master.commit()

                    # append new id and old id to array for csv
                    array_for_csv.append([address_id, old_id])

            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)

    # tests if address exists in database combined
    def address_exist(self, row):
        street = row[0]
        number = row[1]
        local_area_code = row[2]
        city = row[3]

        try:
            with self.con_master.cursor() as cursor:
                # search for address in combined database
                sql = f"""SELECT COUNT(ADRESSE.ADRESSE_ID) FROM ADRESSE WHERE ADRESSE.STRASSE='{street}' AND ADRESSE.PLZ='{local_area_code}' AND ADRESSE.ORT='{city}' AND ADRESSE.HAUSNUMMER='{number}'"""
                cursor.execute(sql)
                result = cursor.fetchone()
                if result[0] > 0:
                    # address exists in database
                    return True

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
        return False

    # search for existing address and get the id
    def get_id_of_existing_address(self, row):
        street = row[0]
        number = row[1]
        local_area_code = row[2]
        city = row[3]

        try:
            # search for address in database and return id
            with self.con_master.cursor() as cursor:
                cursor.execute(f"""SELECT ADRESSE_ID FROM ADRESSE WHERE STRASSE='{street}' AND PLZ='{local_area_code}' AND ORT='{city}' AND HAUSNUMMER='{number}'""")
                result = cursor.fetchone()
                return result[0]

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
        return 0

# create object
addressMerge = AddressMerge()
addressMerge.start()
