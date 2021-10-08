import sqlite3 as sl

from rudolf import config
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException


class SQLiteService:

    def __init__(self):
        self.con = sl.connect(config.RUDOLF_DB_PATH)
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
                            CREATE TABLE if not exists adresse (
                                adresse_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists bestellung (
                                bestellung_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists einkauf (
                                einkauf_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists lieferant_hersteller (
                                lieferant_hersteller_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists marke (
                                marke_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists produkte (
                                produkte_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists kunden (
                                kunden_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists mitarbeiter (
                                mitarbeiter_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists subkategorien (
                                subkategorien_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists zwischenhaendler (
                                zwischenhaendler_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists funktion (
                                funktion_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists lagerplatz (
                                lagerplatz_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER UNIQUE
                            );
                        """)
        except sl.OperationalError as oe:
            print("DB Error")
            print(oe)


if __name__ == "__main__":
    print(SQLiteService().select_where_old_id(config.ADDRESS_DB_TABLE, 6781))  # 6781
