import cx_Oracle
import config
from combined import key_allocation_saver
from combined.ImportKunden.AnredenFinder import AnredenFinder
from combined.key_allocation_reader import read_f2_to_comb_id_allocation_to_file
from util import search_for_id



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

    def generiere_mitarbeiter_csv(self, mitarbeiter_liste):
        id_mapping = []
        for mitarbeiter in mitarbeiter_liste:
            id_mapping.append([mitarbeiter[0], mitarbeiter[1]])
        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(id_mapping,
                                                                   "../../data/allocation_csvs/mitarbeiter.csv")

    def findAdressId(self, alte_adress_id):
        test = read_f2_to_comb_id_allocation_to_file("addresse.csv")
        neueId = search_for_id(test, alte_adress_id)
        return neueId

    def getMitarbeiterF2(self):
        try:
            # get alle Mitarbeiter in F2
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """select MITARBEITER_ID, VORNAME, NACHNAME, ADRESS_ID, GEHALT, PROVISIONSSATZ
                        from MITARBEITER""")
                MitarbeiterList = cursor.fetchall()
                return MitarbeiterList
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
                return FunktionList
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def getZuweisungFunktionMitarbeiterF2(self, mitarbeiterListe):
        return_array = []
        try:
            # get alle ZuweisungFunktionenMitarbeiter in F2
            with self.con_f2.cursor() as cursor:
                for mitarbeiter in mitarbeiterListe:
                    sql = f"""select FUNKTIONS_ID
                            from ZUWEISUNG_MITARBEITER_FUNKTION
                            where MITARBEITER_ID = {mitarbeiter[0]}"""
                    cursor.execute(sql)

                    funktionsid = cursor.fetchall()
                    if (funktionsid):
                        mitarbeiter = list(mitarbeiter)
                        mitarbeiter.append(funktionsid[0][0])
                        return_array.append(mitarbeiter)
            return return_array
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insertFunktionen(self):
        FunktionList = self.getFunktionF2()
        try:
            with self.con_master.cursor() as cursor:
                for funktion in FunktionList:
                    funktionBezeichnung = funktion[0]
                    cursor.execute(f"""INSERT INTO FUNKTION(BEZEICHNUNG) VALUES ('{funktionBezeichnung}')""")
                    self.con_master.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def mitarbeiter_existiert(self, mitarbeiter):
        try:
            with self.con_master.cursor() as cursor:
                mitarbeiter_existiert = False
                # Vorname und Name und Adresse prüfen
                mitarbeiterAdresse = self.findAdressId(mitarbeiter[3])
                cursor.execute(
                    f"""SELECT MITARBEITER_ID FROM MITARBEITER WHERE VORNAME = '{mitarbeiter[1]}' AND NACHNAME = '{mitarbeiter[2]}' AND ADRESS_ID = '{mitarbeiterAdresse}'""")
                dataset = cursor.fetchall()
                if (dataset):
                    mitarbeiter[6] = dataset[0][0]
                    mitarbeiter_existiert = True
                return mitarbeiter_existiert

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insertMitarbeiter(self):
        csv_mitarbeiter_liste = []
        MitarbeiterList = self.getZuweisungFunktionMitarbeiterF2(self.getMitarbeiterF2())
        print(len(MitarbeiterList))
        try:
            for Mitarbeiter in MitarbeiterList:
                print(Mitarbeiter)
                if (self.mitarbeiter_existiert(Mitarbeiter)):
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
                    adressId = int(self.findAdressId(Mitarbeiter[3]))
                    print(adressId)
                    gehalt = Mitarbeiter[4]
                    email = config.DUMMY_MAIL
                    eintrittsdatum = "3333-01-01"
                    anredeFinder = AnredenFinder()
                    anrede = anredeFinder.finde_Anrede(vorname)
                    sql = f"""INSERT INTO MITARBEITER(ANREDE, VORNAME, NACHNAME, EMAIL, GEHALT, EINTRITTSDATUM, ADRESSE_ID)
                                            VALUES ('{anrede}','{vorname}', '{nachname}', '{email}', {gehalt}, TO_DATE('{eintrittsdatum}','yyyy-mm-dd'), {adressId})
                                            returning MITARBEITER_ID into :python_var"""
                    print(sql)
                    with self.con_master.cursor() as cursor:
                        newest_id_wrapper = cursor.var(cx_Oracle.STRING)
                        cursor.execute(sql,
                                       [newest_id_wrapper])
                        mitarbeiter_ID = newest_id_wrapper.getvalue()
                        self.con_master.commit()
                    with self.con_master.cursor() as cursor:
                        cursor.execute(
                            f"""INSERT INTO DATENHERKUNFT_MITARBEITER(MITARBEITER_ID, DATENHERKUNFT_ID) VALUES({mitarbeiter_ID[0]}, 2)""")
                        self.con_master.commit()
                    if Mitarbeiter[5]:
                        with self.con_master.cursor() as cursor:
                            cursor.execute(
                                f"""INSERT INTO PROVISION(MITARBEITER_ID, PROVISIONSSATZ) VALUES({mitarbeiter_ID[0]},{Mitarbeiter[5]})""")
                            self.con_master.commit()
                    with self.con_master.cursor() as cursor:
                        cursor.execute(
                            f"""INSERT INTO ZUWEISUNG_MITARBEITER_FUNKTION(MITARBEITER_ID, FUNKTION_ID) VALUES({mitarbeiter_ID[0]},{Mitarbeiter[6]})""")
                        self.con_master.commit()
                    csv_mitarbeiter_liste.append([int(mitarbeiter_ID[0]), int(Mitarbeiter[0])])
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
        print('exit')
        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(csv_mitarbeiter_liste,
                                                                   "../../data/allocation_csvs/mitarbeiter.csv")
       # self.generiere_mitarbeiter_csv(csv_mitarbeiter_liste)

    def start(self):
        object = Mitarbeiter_Merge()
        object.insertFunktionen()
        object.insertMitarbeiter()


object2 = Mitarbeiter_Merge()
object2.start()

