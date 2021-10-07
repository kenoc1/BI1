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
            self.con.commit()
            return int(newest_id[0])

    def _insert(self, sql_statement: str) -> None:
        with self.con.cursor() as cursor:
            cursor.execute(sql_statement)
            self.con.commit()


class F2DBService(OracleService):

    def __init__(self):
        super().__init__(config.DB_CON_USER_F2, config.DB_CON_PW_F2, config.DB_CON_DSN_F2)

    def select_all_produktoberkategorien(self):
        return self._select_all_dict("PRODUKTOBERKATEGORIE")

    def select_all_produktkategorien(self):
        return self._select_all_dict("PRODUKTKATEGORIE")

    def select_all_addresses(self):
        sql = "SELECT ADRESSE.STRASSE, ADRESSE.NUMMER, ORTSKENNZAHL.ORTSKENNZAHL, ORT.NAME, LAND.BEZEICHNUNG, ADRESSE.ADRESS_ID FROM ADRESSE, ORTSKENNZAHL, ORT, LAND WHERE ADRESSE.ORTSKENNZAHL_ID = ORTSKENNZAHL.ORTSKENNZAHL_ID AND ORTSKENNZAHL.ORT_ID = ORT.ORT_ID AND ORT.LAND_ID = LAND.LAND_ID"
        return self._select_dict(sql)


class CombDBService(OracleService):

    def __init__(self):
        super().__init__(config.DB_CON_USER_COMBINED, config.DB_CON_PW_COMBINED, config.DB_CON_DSN_COMBINED)

    # --------------------Selects--------------------

    def select_produktkategorie_where_bezeichnung(self, bezeichnung: str):
        sql = "select * from PRODUKT_SUBKATEGORIE WHERE BEZEICHNUNG = '{}'".format(bezeichnung)
        return self._select_dict(sql)

    def select_addresses_where(self, strasse: str, plz: int, ort: str, hausnummer: str):
        sql = "select * from ADRESSE where STRASSE='{}' and PLZ='{}' AND ORT='{}' AND HAUSNUMMER='{}'".format(strasse,
                                                                                                              plz, ort,
                                                                                                              hausnummer)
        return self._select_dict(sql)

    # --------------------Inserts--------------------

    def insert_subcategory(self, subcat_name: str):
        sql = "insert into PRODUKT_SUBKATEGORIE(BEZEICHNUNG) values('{}')".format(subcat_name)
        return self._insert_and_return_id(sql, "PRODUKT_SUBKATEGORIE_ID")

    def insert_address(self, land: str, plz: int, ort: str, strasse: str, hausnummer: int, bundesland: str):
        sql = "insert into ADRESSE(LAND, PLZ, ORT, STRASSE, HAUSNUMMER, BUNDESLAND) values('{}', {}, '{}', '{}', {}, " \
              "'{}')".format(land, plz, ort, strasse, hausnummer, bundesland)
        return self._insert_and_return_id(sql, "ADRESSE_ID")

    # --------------------Datenherkunft--------------------

    def insert_subcategory_datenherkunft(self, subcat_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_PRODUKT_SUBKATEGORIE(PRODUKT_SUBKATEGORIE_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(subcat_id, datenherkunft_id)
        self._insert(sql)

    def insert_address_datenherkunft(self, address_id: int, datenherkunft_id: int) -> None:
        sql = "insert into DATENHERKUNFT_ADRESSE(ADRESSE_ID, DATENHERKUNFT_ID) values ({}, " \
              "{})".format(address_id, datenherkunft_id)
        self._insert(sql)
