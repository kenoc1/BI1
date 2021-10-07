import cx_Oracle

import config
import util
from combined.ImportKunden.AnredenFinder import AnredenFinder
from combined.key_allocation_reader import read_f2_to_comb_id_allocation_from_file


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

        self.address_id_allcoation = read_f2_to_comb_id_allocation_from_file(file_name=config.ADDRESS_CON_FILE_NAME)

    # def addProvisionTable(self):
    # add new Table "Provision" im Combined DB
    #   try:
    #      with self.con_master.cursor() as cursor:
    #         cursor.execute("""create table Provision ("Provison_ID" NUMBER constraint TABLE_NAME_PK primary key,
    #                                                      "Provisionssatz" NUMBER(5,2))""")
    #        self.con_master.commit()
    # add FOREIGN KEY to Provision to Mitarbeiter
    #    with self.con_master.cursor() as cursor:
    #       cursor.execute("""ALTER TABLE Provision
    #                         ADD FOREIGN KEY (Mitarbeiter_ID) REFERENCES Mitarbeiter (Mitarbeiter_ID) ON DELETE CASCADE ON UPDATE CASCADE""")
    #    self.con_master.commit()
    # add Zuweisung Prvocion_Mitarbeiter
    # with self.con_master.cursor() as cursor:
    #  cursor.execute("""create table Zuweisung_Provision_Mitarbeiter ("Provison_ID" NUMBER constraint TABLE_NAME_PK primary key,
    #                                                                  "Mitarbeiter_ID" number constraint TABLE_NAME_PK primary key)""")
    # self.con_master.commit()
    # add FOREIGN KEY to Zuweisung_Provision_Mitarbeiter
    # with self.con_master.cursor() as cursor:
    #   cursor.execute("""ALTER TABLE Zuweisung_Provision_Mitarbeiter
    #                              ADD FOREIGN KEY (Mitarbeiter_ID) REFERENCES Mitarbeiter (Mitarbeiter_ID) ON DELETE CASCADE ON UPDATE CASCADE,
    #                             ADD FOREIGN KEY (Provision_ID) REFERENCES Provision (Provision_ID) ON DELETE CASCADE ON UPDATE CASCADE""")
    # self.con_master.commit()

    # except cx_Oracle.Error as error:
    #   print('Error occurred:')
    #  print(error)

    #    def addFunktionTable(self):
    #        # add new Table Funktion
    #        try:
    #            with self.con_master.cursor() as cursor:
    #               cursor.execute("""create table Funktion ("Funktion_ID" NUMBER constraint TABLE_NAME_PK primary key,
    #	                                                            "BEZEICHNUNG" VARCHAR2(128) )""")
    #               self.con_master.commit()
    #            with self.con_master.cursor() as cursor:
    #                cursor.execute("""CREATE TABLE Zuweisung_Mitarbeiter_Funktion (Mitarbeiter_ID Number NOT NULL,
    #                                                               Funktion_ID VARCHAR2 NOT NULL,
    #                                                               PRIMARY KEY (Mitarbeiter_ID, Funktion_ID))""")
    #                # add FOREIGN KEY to Funktion to Mitarbeiter
    #                self.con_master.commit()
    #            with self.con_master.cursor() as cursor:
    #                cursor.execute("""ALTER TABLE Zuweisung_Mitarbeiter_Funktion
    #                                    ADD FOREIGN KEY (Mitarbeiter_ID) REFERENCES Mitarbeiter (Mitarbeiter_ID) ON DELETE CASCADE ON UPDATE CASCADE,
    #                                    ADD FOREIGN KEY (Funktion_ID) REFERENCES Funktion (Funktion_ID) ON DELETE CASCADE ON UPDATE CASCADE""")
    #                self.con_master.commit()
    #        except cx_Oracle.Error as error:
    #            print('Error occurred:')
    #            print(error)

    def find_address_id(self, alte_adress_id):
        return util.search_for_id(self.address_id_allcoation, alte_adress_id)

    def get_mitarbeiter_f2(self):
        try:
            # get alle Mitarbeiter in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Vorname, Nachname, Adress_ID, GEHALT
                        from MITARBEITER
                        order by Adress_ID""")
                mitarbeiter_list = cursor.fetchall()
                for i in mitarbeiter_list:
                    print(i)
                return mitarbeiter_list
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_provision_f2(self):
        try:
            # get alle Provision in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Mitarbeiter_ID, PROVISIONSSATZ
                        from MITARBEITER""")
                provision_list = cursor.fetchall()
                return provision_list
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_funktion_f2(self):
        try:
            # get alle Funktionen in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select BEZEICHNUNG
                        from FUNKTION""")
                funktion_list = cursor.fetchall()
                for funktion in funktion_list:
                    print(funktion)
                return funktion_list
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getZuweisungFunktionMitarbeiterF2(self):
        try:
            # get alle ZuweisungFunktionenMitarbeiter in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select FUNKTIONS_ID, MITARBEITER_ID
                        from ZUWEISUNG_MITARBEITER_FUNKTION""")
                zuweisung_funktion_mitarbeiter_list = cursor.fetchall()
                for zuweisung_funktion_mitarbeiter in zuweisung_funktion_mitarbeiter_list:
                    print(zuweisung_funktion_mitarbeiter)
                return zuweisung_funktion_mitarbeiter_list
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_funktionen(self):
        funktion_list = object.get_funktion_f2()
        try:
            with self.con_master.cursor() as cursor:
                for funktion in funktion_list:
                    # print(funktion)
                    cursor.execute(f"""INSERT INTO FUNKTION(BEZEICHNUNG)
                                    VALUE {funktion}""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_provision(self):
        provision_liste = object.get_provision_f2()
        try:
            with self.con_master.cursor() as cursor:
                for Provision in provision_liste:
                    # print(Provision[1])
                    cursor.execute(f"""INSERT INTO Provision(Provisionssatz)
                                   VALUE ({Provision[1]})""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_mitarbeiter(self):
        mitarbeiter_list = object.get_mitarbeiter_f2()
        try:
            with self.con_master.cursor() as cursor:
                for Mitarbeiter in mitarbeiter_list:
                    vorname = Mitarbeiter[0]
                    nachname = Mitarbeiter[1]
                    adress_id = self.find_address_id(Mitarbeiter[2])
                    gehalt = Mitarbeiter[3]
                    eintrittsdatum = "3333-01-01"
                    # print (vorname, nachname, adressId,gehalt, eintrittsdatum)
                    cursor.execute(f"""INSERT INTO Mitarbeiter(Anrede, Vorname, Nachname, Email, Gehalt, EINTRITTSDATUM)
                                            VALUE ({AnredenFinder(vorname)},{vorname}, {nachname}, {adress_id}, {gehalt}, {eintrittsdatum})""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


if __name__ == "__main__":
    # Frage ZuweisungFunktionMitarbeiter
    # Frage ZuweisungProvisionMitarbeiter

    object = MitarbeiterMerge()
    # templiste = object.addFunktionTable()
    # templiste = object.addProvisionTable()
    # GetMitarbeiterListe = object.get_mitarbeiter_f2()
    # GetFunktionListe = object.get_funktion_f2()
    # GetZuweisungFuktionMitarbeiterListe = object.getZuweisungFunktionMitarbeiterF2()
    # templiste = object.insert_provision(object.get_provision_f2())
    # templiste = object.insert_funktionen()
    # insertMitarbeitertest = object.insert_mitarbeiter()
    # insertProvisiontest = object.insert_provision()
    # insertFunktionentest = object.insert_funktionen()
    # print (GetMitarbeiterListe)
    # print (GetFunktionListe)
    # print (GetZuweisungFuktionMitarbeiterListe)
