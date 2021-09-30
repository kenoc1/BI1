import cx_Oracle
import config
from combined.ImportKunden.AnredenFinder import AnredenFinder
from combined.key_allocation_reader import read_f2_to_comb_id_allocation_to_file


class Mitarbeiter_Merge:
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


    def addProvisionTable(self):
        # add new Table "Provision" im Combined DB
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute("""create table Provision ("Provison_ID" NUMBER constraint TABLE_NAME_PK primary key,
	                                                            "Provisionssatz" NUMBER(5,2))""")
                self.con_master.commit()
        # add FOREIGN KEY to Provision to Mitarbeiter
            with self.con_master.cursor() as cursor:
                cursor.execute("""ALTER TABLE Provision
                                    ADD FOREIGN KEY (Mitarbeiter_ID) REFERENCES Mitarbeiter (Mitarbeiter_ID) ON DELETE CASCADE ON UPDATE CASCADE""")
                self.con_master.commit()
        # add Zuweisung Prvocion_Mitarbeiter
            with self.con_master.cursor() as cursor:
               cursor.execute("""create table Zuweisung_Provision_Mitarbeiter ("Provison_ID" NUMBER constraint TABLE_NAME_PK primary key,
                                                                                "Mitarbeiter_ID" number constraint TABLE_NAME_PK primary key)""")
               self.con_master.commit()
        # add FOREIGN KEY to Zuweisung_Provision_Mitarbeiter
            with self.con_master.cursor() as cursor:
                cursor.execute("""ALTER TABLE Zuweisung_Provision_Mitarbeiter
                                            ADD FOREIGN KEY (Mitarbeiter_ID) REFERENCES Mitarbeiter (Mitarbeiter_ID) ON DELETE CASCADE ON UPDATE CASCADE,
                                            ADD FOREIGN KEY (Provision_ID) REFERENCES Provision (Provision_ID) ON DELETE CASCADE ON UPDATE CASCADE""")
                self.con_master.commit()

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def addFunktionTable(self):
        # add new Table Funktion
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute("""create table Funktion ("Funktion_ID" NUMBER constraint TABLE_NAME_PK primary key,
	                                                            "BEZEICHNUNG" VARCHAR2(128) )""")
                self.con_master.commit()
            with self.con_master.cursor() as cursor:
                cursor.execute("""CREATE TABLE Zuweisung_Mitarbeiter_Funktion (Mitarbeiter_ID Number NOT NULL,
                                                               Funktion_ID VARCHAR2 NOT NULL,
                                                               PRIMARY KEY (Mitarbeiter_ID, Funktion_ID))""")
                # add FOREIGN KEY to Funktion to Mitarbeiter
                self.con_master.commit()
            with self.con_master.cursor() as cursor:
                cursor.execute("""ALTER TABLE Zuweisung_Mitarbeiter_Funktion
                                    ADD FOREIGN KEY (Mitarbeiter_ID) REFERENCES Mitarbeiter (Mitarbeiter_ID) ON DELETE CASCADE ON UPDATE CASCADE,
                                    ADD FOREIGN KEY (Funktion_ID) REFERENCES Funktion (Funktion_ID) ON DELETE CASCADE ON UPDATE CASCADE""")
                self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getCombinedAdressId(self):
        test = read_f2_to_comb_id_allocation_to_file("addresse.csv")
        print(test)

    def getMitarbeiterF2(self):
        try:
            # get alle Mitarbeiter in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Vorname, Nachname, GEHALT
                        from MITARBEITER""")
                MitarbeiterList = cursor.fetchall()
                for i in MitarbeiterList:
                    print (i)
                return MitarbeiterList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getProvisionF2(self):
        try:
            # get alle Provision in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select Mitarbeiter_ID, PROVISIONSSATZ
                        from MITARBEITER""")
                ProvisionList = cursor.fetchall()
                return ProvisionList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getFunktionF2(self):
        try:
            # get alle Funktionen in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select BEZEICHNUNG
                        from FUNKTION""")
                FunktionList = cursor.fetchall()
                for i in FunktionList:
                    print(i)
                return FunktionList
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
                ZuweisungFunktionMitarbeiterList = cursor.fetchall()
                for i in ZuweisungFunktionMitarbeiterList:
                    print(i)
                return ZuweisungFunktionMitarbeiterList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insertFunktionen(self, FunktionList):
        try:
            with self.con_master.cursor() as cursor:
               for funktion in FunktionList:
                    cursor.execute(f"""INSERT INTO FUNKTION(BEZEICHNUNG)
                                    VALUE {funktion}""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


    def insertProvision(self):
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute("""INSERT INTO Provision(Provisionssatz)
                                    VALUE ({ProvisionList})""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


    def insertMitarbeiter(self):
        MitarbeiterList = object.getMitarbeiterF2()
        try:
            with self.con_master.cursor() as cursor:
               for Mitarbeiter in MitarbeiterList:
                    vorname = Mitarbeiter[0]
                    nachname = Mitarbeiter[1]

                    cursor.execute(f"""INSERT INTO Mitarbeiter(Anrede, Vorname, Nachname, Email, Gehalt, EINTRITTSDATUM)
                                            VALUE ({AnredenFinder(vorname)}, )""")
            self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

object = Mitarbeiter_Merge()
#templiste = object.addFunktionTable()
#templiste = object.addProvisionTable()
GetMitarbeiterListe = object.getMitarbeiterF2()
GetFunktionListe = object.getFunktionF2()
GetZuweisungFuktionMitarbeiterListe = object.getZuweisungFunktionMitarbeiterF2()
#templiste = object.insertProvision(object.getProvisionF2())
#templiste = object.insertFunktionen()
print (GetMitarbeiterListe)
print (GetFunktionListe)
print (GetZuweisungFuktionMitarbeiterListe)

test = AnredenFinder()
test.finde_Anrede("Fenja")
print (test.finde_Anrede("Mustafa"))