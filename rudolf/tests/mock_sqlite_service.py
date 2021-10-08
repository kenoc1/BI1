import os
import sqlite3 as sl

from rudolf import config
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException


class MockSQLiteService:

    def __init__(self):
        self.con = sl.connect(config.RUDOLF_TEST_DB_PATH)
        self._setup()

    @staticmethod
    def remove_db():
        os.remove(config.RUDOLF_TEST_DB_PATH)

    def renew_db(self):
        self.remove_db()
        self._setup()

    def insert_id_allocation(self, table_name: str, new_id: int, old_id: int) -> None:
        sql = ''' insert into {} (comb_id, f2_id) values (?,?);'''.format(table_name)
        with self.con:
            cursor = self.con.cursor()
            cursor.execute(sql, (new_id, old_id))

    def select_where_old_id(self, table_name: str, old_id: int) -> int:
        sql = ''' select * from {} where f2_id = {}'''.format(table_name, old_id)
        with self.con:
            cursor = self.con.cursor()
            cursor.execute(sql)
            try:
                (table_id, new_id, old_id) = cursor.fetchone()
                return new_id
            except TypeError:
                raise NoCombIDFoundForF2IDException(old_id)

    def _setup(self):
        try:
            with self.con:
                self.con.execute("""
                            CREATE TABLE adresse (
                                adresse_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE bestellung (
                                bestellung_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE einkauf (
                                einkauf_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE lieferant_hersteller (
                                lieferant_hersteller_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE marke (
                                marke_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE produkte (
                                produkte_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE kunden (
                                kunden_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE mitarbeiter (
                                mitarbeiter_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE subkategorien (
                                subkategorien_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE zwischenhaendler (
                                zwischenhaendler_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE funktion (
                                funktion_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE lagerplatz (
                                lagerplatz_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
        except sl.OperationalError as oe:
            print("DB Error")
            print(oe)
