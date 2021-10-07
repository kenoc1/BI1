import sys

import cx_Oracle

import config
from combined import key_allocation_saver


class InsertKunden:
    def __init__(self):
        self.con_master = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                            dsn=config.DB_CON_DSN_COMBINED, encoding="UTF-8")
        print("Database version:", self.con_master.version)

    def insert_kunden(self, kunden_liste):
        csv_kunden_liste = []
        with self.con_master.cursor() as cursor:
            if kunden_liste:
                for kunde in kunden_liste:
                    if self.kunde_existiert(kunde):
                        # Kunde ist schon vorhanden, nur Eintrag in die Zwischentabelle
                        print('Kunde mit der ID ', kunde.id_combined, 'existiert bereits, wird übersprungen.')
                        cursor.execute(
                            f"""INSERT INTO DATENHERKUNFT_KUNDE(KUNDE_ID, DATENHERKUNFT_ID) VALUES({kunde.id_combined}, 2)""")
                        self.con_master.commit()
                        csv_kunden_liste.append([kunde.id_combined, kunde.id_filiale])
                        print("Bestehender Kunde wurde eingefügt")
                    else:
                        try:
                            # Kunde existiert noch nicht, muss eingefügt werden
                            cursor.execute(
                                f"""INSERT INTO KUNDE(ANREDE, VORNAME, NACHNAME, EMAIL, GEBURTSDATUM) VALUES ('{kunde.anrede}','{kunde.vorname}','{kunde.nachname}', '{kunde.email}', TO_DATE('{self.datetime_zu_date(kunde.geburtsdatum)}','yyyy-mm-dd'))""")
                            self.con_master.commit()
                            # Neue ID zum Kunden hinzufügen
                            kunde.id_combined = self.get_combined_Id(kunde)
                            print("Current KundenID", kunde.id_combined)
                            print("Vorherige KundenID", kunde.id_filiale)
                            cursor.execute(
                                f"""INSERT INTO DATENHERKUNFT_KUNDE(KUNDE_ID, DATENHERKUNFT_ID) VALUES({kunde.id_combined}, 2)""")
                            self.con_master.commit()

                            # Beziehungen zu Adressen hinzufügen
                            if kunde.lieferadresse_id:
                                lieferadresse = "Lieferadresse"
                                cursor.execute(
                                    f"""INSERT INTO KUNDE_ADRESSE(ADRESSE_ID, KUNDE_ID, ADRESSART) VALUES ({kunde.lieferadresse_id},{kunde.id_combined}, '{lieferadresse}')""")
                                self.con_master.commit()
                            if kunde.rechnungsadresse_id:
                                rechnungsadresse = "Rechnungsadresse"
                                cursor.execute(
                                    f"""INSERT INTO KUNDE_ADRESSE(ADRESSE_ID, KUNDE_ID, ADRESSART) VALUES ({kunde.rechnungsadresse_id},{kunde.id_combined}, '{rechnungsadresse}')""")
                                self.con_master.commit()
                            # 4. Eintrag in WARENKORB mit GESAMTPREIS = 0
                            gesamt_preis = 0
                            cursor.execute(
                                f"""INSERT INTO WARENKORB(KUNDE_ID, GESAMTPREIS)  VALUES ({kunde.id_combined}, {gesamt_preis})""")
                            self.con_master.commit()
                            # Adresse in die Liste für den csv-Export hinzufügen
                            csv_kunden_liste.append([kunde.id_combined, kunde.id_filiale])
                            print('Kunde mit der ID ', kunde.id_combined, ' wurde erfolgreich angelegt.')
                        except cx_Oracle.Error as error:
                            print('Error occurred:')
                            print(error)
                            sys.exit(0)
                self.generiere_kunden_csv(csv_kunden_liste)

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
                if dataset:
                    # Vor- und Nachname sind bereits vorhanden, Rechnungsadresse prüfen
                    cursor.execute(
                        f"""SELECT COUNT(K.KUNDE_ID) FROM KUNDE_ADRESSE KA WHERE KA.KUNDE_ID = '{kunde.id_combined}' AND KA.ADRESSE_ID = {kunde.rechnungsadresse_id} AND KA.ADRESSART = 'Rechnungsadresse'""")
                    dataset = cursor.fetchall()
                if dataset:
                    # Rechnungsadresse passt auch, also existiert Kunde
                    kunde_existiert = True
                    kunde.set_id_combined(dataset[0][0])
                return kunde_existiert

        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def generiere_kunden_csv(self, kunden_liste):
        id_mapping = []
        for kunde in kunden_liste:
            id_mapping.append([kunde[0], kunde[1]])
        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(id_mapping, "../../data/allocation_csvs/kunden.csv")

    def get_combined_Id(self, kunde):
        with self.con_master.cursor() as cursor:
            # Vorname und Name prüfen
            try:
                cursor.execute(
                    f"""SELECT K.KUNDE_ID FROM KUNDE K WHERE
                     K.VORNAME = '{kunde.vorname}' 
                    AND K.NACHNAME = '{kunde.nachname}'
                    AND K.EMAIL = '{kunde.email}'""")
                id_in_list = cursor.fetchall()
                return id_in_list[0][0]
            except cx_Oracle.Error as error:
                print('Error occurred:')
                print(error)
                sys.exit(0)

    def datetime_zu_date(self, datetime):
        return str(datetime).split()[0]
