import cx_Oracle

import config
from combined import key_allocation_saver
from combined.customers.AnredenFinder import AnredenFinder
from combined.key_allocation_reader import read_f2_to_comb_id_allocation_from_file
from util import search_for_id


class MitarbeiterMerge:
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

    @staticmethod
    def generiere_mitarbeiter_csv(mitarbeiter_liste):
        id_mapping = []
        for mitarbeiter in mitarbeiter_liste:
            id_mapping.append([mitarbeiter[0], mitarbeiter[1]])
        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(id_mapping,
                                                                   "../../data/allocation_csvs/mitarbeiter.csv")

    @staticmethod
    def find_new_address_id(alte_adress_id):
        test = read_f2_to_comb_id_allocation_from_file("addresse.csv")
        neue_id = search_for_id(test, alte_adress_id)
        return neue_id

    def get_all_f2_mitarbeiter(self):
        try:
            # get alle Mitarbeiter in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select MITARBEITER_ID, VORNAME, NACHNAME, ADRESS_ID, GEHALT, PROVISIONSSATZ
                        from MITARBEITER""")
                mitarbeiter_list = cursor.fetchall()
                return mitarbeiter_list
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_all_f2_funktion(self):
        try:
            # get alle Funktionen in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select BEZEICHNUNG
                        from FUNKTION""")
                FunktionList = cursor.fetchall()
                return FunktionList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_zuweisung_funktion_mitarbeiter_for_mitarbeiter_f2(self, mitarbeiter_liste):
        return_array = []
        try:
            # get alle ZuweisungFunktionenMitarbeiter in F2
            with self.con_f2.cursor() as cursor:
                for mitarbeiter in mitarbeiter_liste:
                    sql = f"""select FUNKTIONS_ID
                            from ZUWEISUNG_MITARBEITER_FUNKTION
                            where MITARBEITER_ID = {mitarbeiter[0]}"""
                    cursor.execute(sql)

                    funktionsid = cursor.fetchall()
                    if funktionsid:
                        mitarbeiter = list(mitarbeiter)
                        mitarbeiter.append(funktionsid[0][0])
                        return_array.append(mitarbeiter)
            return return_array
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_funkionen(self):
        funktion_list = self.get_all_f2_funktion()
        try:
            with self.con_master.cursor() as cursor:
                for funktion in funktion_list:
                    funktion_bezeichnung = funktion[0]
                    cursor.execute(f"""INSERT INTO FUNKTION(BEZEICHNUNG) VALUES ('{funktion_bezeichnung}')""")
                    self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def mitarbeiter_existiert(self, mitarbeiter):
        try:
            with self.con_master.cursor() as cursor:
                mitarbeiter_existiert = False
                # Vorname und Name und Adresse prüfen
                mitarbeiter_adresse = self.find_new_address_id(mitarbeiter[3])
                cursor.execute(
                    f"""SELECT MITARBEITER_ID FROM MITARBEITER WHERE VORNAME = '{mitarbeiter[1]}' AND NACHNAME = '{mitarbeiter[2]}' AND ADRESS_ID = '{mitarbeiter_adresse}'""")
                dataset = cursor.fetchall()
                if dataset:
                    mitarbeiter[6] = dataset[0][0]
                    mitarbeiter_existiert = True
                return mitarbeiter_existiert

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_mitarbeiter(self):
        csv_mitarbeiter_liste = []
        mitarbeiter_list = self.get_zuweisung_funktion_mitarbeiter_for_mitarbeiter_f2(self.get_all_f2_mitarbeiter())
        print(len(mitarbeiter_list))
        try:
            for Mitarbeiter in mitarbeiter_list:
                print(Mitarbeiter)
                if self.mitarbeiter_existiert(Mitarbeiter):
                    print('Mitarbeiter mit der ID ', Mitarbeiter[6], 'existiert bereits, wird übersprungen.')
                    cursor.execute(
                        f"""INSERT INTO DATENHERKUNFT_MITARBEITER(MITARBEITER_ID, DATENHERKUNFT_ID) VALUES({Mitarbeiter[6]}, 2)""")
                    self.con_master.commit()
                    cursor.execute(
                        f"""INSERT INTO PROVISION(MITARBEITER_ID, PROVISIONSSATZ) VALUES({Mitarbeiter[6]},{Mitarbeiter[5]})""")
                    self.con_master.commit()
                    cursor.execute(
                        f"""INSERT INTO ZUWEISUNG_MITARBEITER_FUNKTION(MITARBEITER_ID, FUNKTION_ID) VALUES({Mitarbeiter[6]},{Mitarbeiter[7]})""")
                    self.con_master.commit()
                    csv_mitarbeiter_liste.append([Mitarbeiter[6], Mitarbeiter[0]])
                    print("Bestehender Mitarbeiter wurde eingefügt")
                else:
                    vorname = Mitarbeiter[1]
                    nachname = Mitarbeiter[2]
                    adress_id = int(self.find_new_address_id(Mitarbeiter[3]))
                    print(adress_id)
                    gehalt = Mitarbeiter[4]
                    email = config.DUMMY_MAIL
                    eintrittsdatum = "3333-01-01"
                    anrede_finder = AnredenFinder()
                    anrede = anrede_finder.finde_anrede(vorname)
                    sql = f"""INSERT INTO MITARBEITER(ANREDE, VORNAME, NACHNAME, EMAIL, GEHALT, EINTRITTSDATUM, ADRESSE_ID)
                                            VALUES ('{anrede}','{vorname}', '{nachname}', '{email}', {gehalt}, TO_DATE('{eintrittsdatum}','yyyy-mm-dd'), {adress_id})
                                            returning MITARBEITER_ID into :python_var"""
                    print(sql)
                    with self.con_master.cursor() as cursor:
                        newest_id_wrapper = cursor.var(cx_Oracle.STRING)
                        cursor.execute(sql,
                                       [newest_id_wrapper])
                        mitarbeiter_id = newest_id_wrapper.getvalue()
                        self.con_master.commit()
                    with self.con_master.cursor() as cursor:
                        cursor.execute(
                            f"""INSERT INTO DATENHERKUNFT_MITARBEITER(MITARBEITER_ID, DATENHERKUNFT_ID) VALUES({mitarbeiter_id[0]}, 2)""")
                        self.con_master.commit()
                    if Mitarbeiter[5]:
                        with self.con_master.cursor() as cursor:
                            cursor.execute(
                                f"""INSERT INTO PROVISION(MITARBEITER_ID, PROVISIONSSATZ) VALUES({mitarbeiter_id[0]},{Mitarbeiter[5]})""")
                            self.con_master.commit()
                    with self.con_master.cursor() as cursor:
                        cursor.execute(
                            f"""INSERT INTO ZUWEISUNG_MITARBEITER_FUNKTION(MITARBEITER_ID, FUNKTION_ID) VALUES({mitarbeiter_id[0]},{Mitarbeiter[6]})""")
                        self.con_master.commit()
                    csv_mitarbeiter_liste.append([int(mitarbeiter_id[0]), int(Mitarbeiter[0])])
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
        print('exit')
        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(csv_mitarbeiter_liste,
                                                                   "../../data/allocation_csvs/mitarbeiter.csv")

    # self.generiere_mitarbeiter_csv(csv_mitarbeiter_liste)

    def start(self):
        self.insert_funkionen()
        self.insert_mitarbeiter()


if __name__ == "__main__":
    mitarbeiter_merger = MitarbeiterMerge()
    mitarbeiter_merger.start()
