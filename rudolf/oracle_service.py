import cx_Oracle

import config


class OracleService:

    def __init__(self, user: str, password: str, dsn: str):
        self.con = cx_Oracle.connect(user=user, password=password, dsn=dsn, encoding="UTF-8")

    def _select_all_dict(self, table_name: str) -> list[dict]:
        with self.con.cursor() as cursor:
            cursor.execute("select * from {}".format(table_name))
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            rows = cursor.fetchall()
            if rows:
                return rows
            else:
                return []

    def _select_dict(self, sql_statement: str) -> list[dict]:
        with self.con.cursor() as cursor:
            cursor.execute(sql_statement)
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            rows = cursor.fetchall()
            if rows:
                return rows
            else:
                return []

    def _insert_and_return_id(self, sql_statement: str, id_name: str) -> int:
        sql = sql_statement + "returning {} into :python_var".format(id_name)
        with self.con.cursor() as cursor:
            newest_id_wrapper = cursor.var(cx_Oracle.STRING)
            cursor.execute(sql, [newest_id_wrapper])
            newest_id = newest_id_wrapper.getvalue()
            return int(newest_id[0])

    def _insert(self, sql_statement: str) -> None:
        with self.con.cursor() as cursor:
            cursor.execute(sql_statement)
            self.con.commit()


class F2DBService(OracleService):

    def __init__(self):
        super().__init__(config.DB_CON_USER_F2, config.DB_CON_PW_F2, config.DB_CON_DSN_F2)

    def select_all_produktoberkategorien(self) -> list[dict]:
        return self._select_all_dict("PRODUKTOBERKATEGORIE")

    def select_all_produktkategorien(self) -> list[dict]:
        return self._select_all_dict("PRODUKTKATEGORIE")

    def select_all_addresses_join(self) -> list[dict]:
        sql = "SELECT ADRESSE.STRASSE, ADRESSE.NUMMER, ORTSKENNZAHL.ORTSKENNZAHL, ORT.NAME, LAND.BEZEICHNUNG, " \
              "ADRESSE.ADRESS_ID FROM ADRESSE, ORTSKENNZAHL, ORT, LAND " \
              "WHERE ADRESSE.ORTSKENNZAHL_ID = ORTSKENNZAHL.ORTSKENNZAHL_ID " \
              "AND ORTSKENNZAHL.ORT_ID = ORT.ORT_ID " \
              "AND ORT.LAND_ID = LAND.LAND_ID"
        return self._select_dict(sql)

    def select_all_hersteller(self) -> list[dict]:
        return self._select_all_dict("HERSTELLER")

    def select_all_funktionen(self) -> list[dict]:
        return self._select_all_dict("FUNKTION")

    def select_all_mitarbeiter(self) -> list[dict]:
        return self._select_all_dict("MITARBEITER")

    def select_all_mitarbeiter_join_funktion(self) -> list[dict]:
        sql = "SELECT * FROM MITARBEITER m " \
              "JOIN ZUWEISUNG_MITARBEITER_FUNKTION  z ON m.MITARBEITER_ID = z.MITARBEITER_ID " \
              "JOIN FUNKTION  f ON z.FUNKTIONS_ID = f.FUNKTIONS_ID "
        return self._select_dict(sql)

    def select_all_lagerplaetze_join_produkt(self) -> list[dict]:
        sql = "select * from LAGER_EINHEIT LF2,ZUWEISUNG_PRODUKT_LAGERPLATZ ZF2 WHERE LF2.LAGERPLATZ_ID = ZF2.LAGERPLATZ_ID"
        return self._select_dict(sql)

    def select_all_kunden(self) -> list[dict]:
        return self._select_all_dict("KUNDE")

    def select_all_sales(self):
        return self._select_all_dict("VERKAUF")

    def select_all_rechnung(self):
        return self._select_all_dict("RECHNUNG")

    def select_all_lieferschein(self):
        return self._select_all_dict("LIEFERSCHEIN")

    def select_all_bons(self):
        return self._select_all_dict("BON")

    def select_all_gewichtsbasiert_verkauf(self):
        return self._select_all_dict("GEWICHTBASIERTES_PRODUKT_IM_VERKAUF")

    def select_all_stueckzahlbasiert_verkauf(self):
        return self._select_all_dict("STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF")


class CombDBService(OracleService):

    def __init__(self):
        super().__init__(config.DB_CON_USER_COMBINED, config.DB_CON_PW_COMBINED, config.DB_CON_DSN_COMBINED)

    # --------------------Selects--------------------

    def select_all_funktionen(self) -> list[dict]:
        return self._select_all_dict("FUNKTION")

    def select_all_addresses(self) -> list[dict]:
        return self._select_all_dict("ADRESSE")

    def select_all_lieferanten(self) -> list[dict]:
        return self._select_all_dict("LIEFERANT")

    def select_all_mitarbeiter(self) -> list[dict]:
        return self._select_all_dict("MITARBEITER")

    def select_produktkategorie_where_bezeichnung(self, bezeichnung: str) -> list[dict]:
        sql = "select * from PRODUKT_SUBKATEGORIE WHERE BEZEICHNUNG = '{}'".format(bezeichnung)
        return self._select_dict(sql)

    def select_addresses_where(self, strasse: str, plz: int, ort: str, hausnummer: str) -> list[dict]:
        sql = "select * from ADRESSE where STRASSE='{}' and PLZ='{}' AND ORT='{}' AND HAUSNUMMER='{}'".format(strasse,
                                                                                                              plz, ort,
                                                                                                              hausnummer)
        return self._select_dict(sql)

    def select_all_kunden_join_adresse_where_rechnungsadresse(self) -> list[dict]:
        sql = "select * from KUNDE k JOIN KUNDE_ADRESSE a ON k.KUNDE_ID = a.KUNDE_ID WHERE a.ADRESSART = 'Rechnungsadresse'"
        return self._select_dict(sql)

    def select_all_warenkoerbe(self):
        return self._select_all_dict("WARENKORB")

    # --------------------Inserts--------------------

    def insert_subcategory(self, subcat_name: str) -> int:
        sql = "insert into PRODUKT_SUBKATEGORIE(BEZEICHNUNG) values('{}')".format(subcat_name)
        return self._insert_and_return_id(sql, "PRODUKT_SUBKATEGORIE_ID")

    def insert_address(self, land: str, plz: int, ort: str, strasse: str, hausnummer: int, bundesland: str) \
            -> int:
        sql = "insert into ADRESSE(LAND, PLZ, ORT, STRASSE, HAUSNUMMER, BUNDESLAND) values('{}', {}, '{}', '{}', {}, " \
              "'{}')".format(land, plz, ort, strasse, hausnummer, bundesland)
        return self._insert_and_return_id(sql, "ADRESSE_ID")

    def insert_lieferant(self, adresse_id: int, lieferant_name: str, email: str) -> int:
        sql = "insert into LIEFERANT(ADRESSE_ID, LIEFERANT_NAME, EMAIL) values( {}, '{}', '{}')" \
            .format(adresse_id, lieferant_name, email)
        return self._insert_and_return_id(sql, "LIEFERANT_ID")

    def insert_funktion(self, bezeichnung: str) -> int:
        sql = "insert into FUNKTION(BEZEICHNUNG) values('{}')" \
            .format(bezeichnung)
        return self._insert_and_return_id(sql, "FUNKTION_ID")

    def insert_mitarbeiter(self, anrede: str, vorname: str, nachname, email: str, gehalt: float, eintrittsdatum: str,
                           adresse_id: int) -> int:
        sql = "insert into MITARBEITER(ANREDE, VORNAME, NACHNAME, EMAIL, GEHALT, EINTRITTSDATUM, ADRESSE_ID) " \
              "VALUES ('{}','{}', '{}', '{}', {}, TO_DATE('{}','yyyy-mm-dd'), {})" \
            .format(anrede, vorname, nachname, email, gehalt, eintrittsdatum, adresse_id)
        return self._insert_and_return_id(sql, "MITARBEITER_ID")

    def insert_mitarbeiter_provision(self, mitarbeiter_id: int, provisionssatz: float) -> int:
        sql = "insert into PROVISION(MITARBEITER_ID, PROVISIONSSATZ) values({},{})" \
            .format(mitarbeiter_id, provisionssatz)
        return self._insert_and_return_id(sql, "PROVISION_ID")

    def insert_mitarbeiter_funktion(self, mitarbeiter_id: int, funktion_id: int) -> int:
        sql = "insert into ZUWEISUNG_MITARBEITER_FUNKTION(MITARBEITER_ID, FUNKTION_ID) values({},{})" \
            .format(mitarbeiter_id, funktion_id)
        return self._insert_and_return_id(sql, "ZUWEISUNG_MITARBEITER_FUNKTION_ID")

    def insert_lagerplatz(self, lager_id: int, produkt_id: int, regal_reihe: int, regal_spalte: int, akt_menge: int,
                          regal_zeile: int) -> int:
        sql = "insert into LAGERPLATZ(LAGER_ID, PRODUKT_ID, REGAL_REIHE, REGAL_SPALTE, AKT_MENGE, REGAL_ZEILE) " \
              " values ({}, {}, {}, {}, {}, {})" \
            .format(lager_id, produkt_id, regal_reihe, regal_spalte, akt_menge, regal_zeile)
        return self._insert_and_return_id(sql, "LAGERPLATZ_ID")

    def insert_kunde(self, anrede: str, vorname: str, nachname: str, email: str, geburtsdatum: str) -> int:
        sql = "INSERT INTO KUNDE(ANREDE, VORNAME, NACHNAME, EMAIL, GEBURTSDATUM) " \
              "VALUES ('{}','{}','{}', '{}', TO_DATE('{}','yyyy-mm-dd'))" \
            .format(anrede, vorname, nachname, email, geburtsdatum)
        return self._insert_and_return_id(sql, "KUNDE_ID")

    def insert_kunde_adresse(self, adress_id: int, kunden_id: int, adressart: str) -> int:
        sql = "INSERT INTO KUNDE_ADRESSE(ADRESSE_ID, KUNDE_ID, ADRESSART) VALUES ({}, {}, '{}')".format(adress_id,
                                                                                                        kunden_id,
                                                                                                        adressart)
        return self._insert_and_return_id(sql, "KUNDE_ADRESSE_ID")

    def insert_warenkorb(self, kunden_id: int, gesamtpreis: float) -> int:
        sql = "INSERT INTO WARENKORB(KUNDE_ID, GESAMTPREIS) " \
              "VALUES ({}, {})".format(kunden_id, gesamtpreis)
        return self._insert_and_return_id(sql, "WARENKORB_ID")

    # TODO TEST
    def insert_bestellung(self, warenkorb_id: int, status: str, bestelldatum, datenherkunft: int,
                          mitarbeiter_id: int) -> int:
        sql = ("insert into BESTELLUNG(WARENKORB_ID, STATUS, BESTELLDATUM, DATENHERKUNFT_ID, MITARBEITER_ID) "
               "values({}, '{}', TO_DATE('{}','yyyy-mm-dd'), {}, {})").format(warenkorb_id, status, bestelldatum,
                                                                              datenherkunft, mitarbeiter_id)
        return self._insert_and_return_id(sql, "BESTELLUNG_ID")

    def insert_bestellung_to_zahlungsart(self, bestellungid: int, zahlungsart_id: int):
        sql = ("insert into ZAHLUNGSART_BESTELLUNG(BESTELLUNG_ID , ZAHLUNGSART_ID) "
               "values({}, {}) ").format(bestellungid, zahlungsart_id)
        self._insert(sql)

    def insert_rechnung(self, bestellungid: int, rechnungsdatum, summe_netto: float, summe_brutto: float):
        sql = ("insert into RECHNUNG(BESTELLUNG_ID, RECHNUNGSDATUM, SUMME_NETTO, SUMME_BRUTTO) "
               "values({}, TO_DATE('{}','yyyy-mm-dd'), {}, {})") \
            .format(bestellungid, rechnungsdatum, summe_netto, summe_brutto)
        return self._insert_and_return_id(sql, "RECHNUNG_ID")

    def insert_bon(self, bestellungid: int, gegebenesgeld: float, rueckgeld: float, summe_netto: float,
                   summe_brutto: float):
        sql = ("insert into BON(BESTELLUNG_ID, GEGEBENES_GELD, RUECKGELD,SUMME_NETTO, SUMME_BRUTTO) "
               "values({}, {}, {}, {}, {})") \
            .format(bestellungid, gegebenesgeld, rueckgeld, summe_netto, summe_brutto)
        return self._insert_and_return_id(sql, "BON_ID")

    def insert_lieferschein(self, bestellungid: int, lieferkosten: float, lieferdatum):
        sql = ("insert into LIEFERSCHEIN(BESTELLUNG_ID, LIEFERKOSTEN , LIEFERDATUM) "
               "values({}, {}, TO_DATE('{}','yyyy-mm-dd'))").format(bestellungid, lieferkosten, lieferdatum)
        return self._insert_and_return_id(sql, "LIEFERSCHEIN_ID")

    def insert_bestellposition(self, bestellungid: int, produktid: int, menge: int):
        sql = (
            "insert into BESTELLPOSITION(PRODUKT_ID, BESTELLUNG_ID , MENGE) "
            "values({}, {}, {})").format(produktid, bestellungid, menge)
        self._insert(sql)

    # --------------------Datenherkunft--------------------

    def insert_subcategory_datenherkunft(self, subcat_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_PRODUKT_SUBKATEGORIE(PRODUKT_SUBKATEGORIE_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(subcat_id, datenherkunft_id)
        self._insert(sql)

    def insert_address_datenherkunft(self, address_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_ADRESSE(ADRESSE_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(address_id, datenherkunft_id)
        self._insert(sql)

    def insert_lieferant_datenherkunft(self, lieferant_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_LIEFERANT(LIEFERANT_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(lieferant_id, datenherkunft_id)
        self._insert(sql)

    def insert_funktion_datenherkunft(self, funktion_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_FUNKTION(FUNKTION_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(funktion_id, datenherkunft_id)
        self._insert(sql)

    def insert_mitarbeiter_datenherkunft(self, mitarbeiter_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_MITARBEITER(MITARBEITER_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(mitarbeiter_id, datenherkunft_id)
        self._insert(sql)

    def insert_kunde_datenherkunft(self, kunden_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_KUNDE(KUNDE_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(kunden_id, datenherkunft_id)
        self._insert(sql)


if __name__ == "__main__":
    # print(F2DBService().select_all_kunden())
    print(CombDBService().select_all_kunden_join_adresse_where_rechnungsadresse())
