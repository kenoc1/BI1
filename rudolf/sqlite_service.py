import sqlite3 as sl


class SQLiteService:

    def __init__(self):
        self.con = sl.connect('rudolf-db.db')
        self.setup()

    def setup(self):
        try:
            with self.con:
                self.con.execute("""
                            CREATE TABLE if not exists adresse (
                                adresse_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists bestellung (
                                bestellung_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists einkauf (
                                einkauf_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists lieferant_hersteller (
                                lieferant_hersteller_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists marke (
                                marke_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists produkte (
                                produkte_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists kunden (
                                produkte_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists mitarbeiter (
                                mitarbeiter_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists subkategorien (
                                subkategorien_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
                self.con.execute("""
                            CREATE TABLE if not exists zwischenhaendler (
                                zwischenhaendler_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                comb_id INTEGER,
                                f2_id INTEGER
                            );
                        """)
        except sl.OperationalError as oe:
            print("DB Error")
            print(oe)


if __name__ == "__main__":
    SQLiteService()
