import cx_Oracle

import config
from combined.ImportKunden.Kunde import Kunde
from combined.ImportKunden.AnredenFinder import AnredenFinder


class ImportKunden:
    def __init__(self):
        # Liste in der die selektierten Kunden gespeichert werden
        self.kunden_tupel = []
        self.kunden_objekte = []

        # Liste in denen die selektierten Adressen gespeichert werden
        self.lieferadress_tupel = []
        self.rechnungsadress_tupel = []
        self.adress_objekte = []

        # DB-Verbindung zu F2
        self.con_f2 = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")
        print("Database version:", self.con_f2.version)

        # DB-Verbindung zum neuen Schema Combined
        self.con_master = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                            dsn=config.DB_CON_DSN_COMBINED, encoding="UTF-8")
        print("Database version:", self.con_master.version)

    def start_import(self):
        self.select_kunden()
        if self.kunden_tupel:
            return self.get_kunden_objekte()
        else:
            print('Fehler: Kunden konnten nicht importiert werden!')

    def select_kunden(self):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    """SELECT KUNDE.KUNDEN_ID, KUNDE.VORNAME, KUNDE.NACHNAME, KUNDE.GEBURTSDATUM FROM KUNDE""")
                dataset = cursor.fetchall()
                if (dataset):
                    self.kunden_tupel = dataset
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    # Erzeugt aus den zuvor importierten Kunden_Tupeln eine Liste von Kunden_Objekten mit den entsprechenden Attributen
    def get_kunden_objekte(self):
        self.select_kunden()
        anreden_finder = AnredenFinder()
        dummy_mail = 'Keine E-Mail'
        for tupel in self.kunden_tupel:
            kunde = Kunde()
            # Anrede herausfinden
            kunde.set_id_filiale(tupel[0])
            kunde.set_vorname(tupel[1])
            kunde.set_anrede(anreden_finder.finde_Anrede(kunde.vorname))
            kunde.set_nachname(tupel[2])
            kunde.set_geburtsdatum(tupel[3])
            kunde.set_email(dummy_mail)
            self.kunden_objekte.append(kunde)

        return self.kunden_objekte


