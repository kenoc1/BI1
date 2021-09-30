import Kunde
import cx_Oracle

import config


class InsertKunden:
    def __init__(self):
        self.con_master = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                            dsn=config.DB_CON_DSN_COMBINED, encoding="UTF-8")
        print("Database version:", self.con_master.version)

    def insert_kunden(self, kunden_liste):
        global counter
        if (kunden_liste):
            # Hier wird die Kundenliste durchgegangen und jeder Kunde überprüft, ob er bereits existiert
            for kunde in kunden_liste:
                if(self.kunde_existiert(kunde)):
                    # Kunde ist schon vorhanden, dann nicht inserten, sondern nur Eintrag in die Zwischentabelle
                    print('Kunde existiert, hier würde Eintrag in die Zwischentabelle geschrieben werden')
                else:
                    # Kunde existiert noch nicht, muss eingefügt werden
                    # 1. Eintrag in KUNDE
                    # 2. CSV generieren
                    # 3. Eintrag in KUNDE_ADRESSE
                    # 4. Eintrag in WARENKORB mit GESAMTPREIS = 0
                    print('Hier wird alles angelegt')


    # Prüft, ob die Kunden schon in der Datenbank vorhanden sind
    # Name = Name, Vorname = Vorname, Adress_Id = Adress_Id (muss gemappt sein, also schon die neue ID!)
    def kunde_existiert(self, kunde):
        try:
            with self.con_master.cursor() as cursor:
                kunde_existiert = False
                # Vorname und Name prüfen
                cursor.execute(
                    f"""SELECT K.KUNDE_ID FROM KUNDE K WHERE K.VORNAME = '{kunde.vorname}' AND K.NACHNAME = '{kunde.nachname}'""")
                dataset = cursor.fetchall()
                if (dataset):
                    # Vor- und Nachname sind bereits vorhanden, Rechnungsadresse prüfen
                    kunde.set_id_combined(dataset)
                    cursor.execute(
                        f"""SELECT COUNT(K.KUNDE_ID) FROM KUNDE_ADRESSE KA WHERE KA.KUNDE_ID = '{kunde.id_combined}' AND KA.ADRESSE_ID = '{kunde.rechnungsadresse}' AND KA.ADRESSART = 'Rechnungsadresse'""")
                    dataset = cursor.fetchall()
                if(dataset):
                    # Rechnungsadresse passt auch, also existiert Kunde
                    kunde_existiert = True
                return kunde_existiert

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)
