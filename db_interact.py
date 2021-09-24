from random import randint
import cx_Oracle
import config


class DB:
    def __init__(self):
        self.con = cx_Oracle.connect(user=config.DB_CON_USER, password=config.DB_CON_PW, dsn=config.DB_CON_DSN,
                                     encoding="UTF-8")
        print("Database version:", self.con.version)

    def select_table(self, table_name):
        try:
            with self.con.cursor() as cursor:
                cursor.execute(f"""select * from {table_name}""")
                rows = cursor.fetchall()
                if rows:
                    return rows
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_product_with_id(self, product_id):
        try:
            with self.con.cursor() as cursor:
                cursor.execute(f"""select * from {config.PRODUCTS} where PRODUKT_ID = :product_id""",
                               product_id=product_id)
                cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
                rows = cursor.fetchall()
                if rows:
                    return rows[0]
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def customer_present(self, customer_id):
        with self.con.cursor() as cursor:
            cursor.execute("""select * from KUNDE WHERE KUNDEN_ID = :customer_id""", customer_id=customer_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def get_start_customer_id(self):
        try:
            with self.con.cursor() as cursor:
                cursor.execute("""select MIN(KUNDEN_ID) from KUNDE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_end_customer_id(self):
        try:
            with self.con.cursor() as cursor:
                cursor.execute("""select MAX(KUNDEN_ID) from KUNDE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_address_id_min(self) -> int:
        try:
            with self.con.cursor() as cursor:
                cursor.execute("""select MIN(ADRESS_ID) from ADRESSE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_address_id_max(self) -> int:
        try:
            with self.con.cursor() as cursor:
                cursor.execute("""select MAX(ADRESS_ID) from ADRESSE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_random_seller(self):
        try:
            with self.con.cursor() as cursor:
                cursor.execute("""  SELECT distinct m.MITARBEITER_ID FROM MITARBEITER m, FUNKTION f,
                                    ZUWEISUNG_MITARBEITER_FUNKTION z WHERE f.FUNKTIONS_ID = z.FUNKTIONS_ID   
                                    AND z.MITARBEITER_ID = m.MITARBEITER_ID
                                    AND (f.BEZEICHNUNG = 'Kassierer*in' OR f.BEZEICHNUNG = 'Oberkassierer*in')""")
                row = cursor.fetchall()
                if row:
                    i = randint(0, len(row) - 1)
                    return row[i][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_random_buyer(self):
        try:
            with self.con.cursor() as cursor:
                cursor.execute("""  SELECT distinct m.MITARBEITER_ID FROM MITARBEITER m, FUNKTION f,
                                    ZUWEISUNG_MITARBEITER_FUNKTION z WHERE f.FUNKTIONS_ID = z.FUNKTIONS_ID   
                                    AND z.MITARBEITER_ID = m.MITARBEITER_ID
                                    AND (f.BEZEICHNUNG = 'Einkäufer')""")
                row = cursor.fetchall()
                if row:
                    i = randint(0, len(row) - 1)
                    return row[i][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_random_customer(self):
        start_customer_id = self.get_start_customer_id()
        end_customer_id = self.get_end_customer_id()
        present_customer = False
        while not present_customer:
            customer_id = randint(start_customer_id, end_customer_id)
            present_customer = self.customer_present(customer_id)

    def worker_present(self, worker_id):
        with self.con.cursor() as cursor:
            cursor.execute("""select * from KUNDE WHERE KUNDEN_ID = :customer_id""", customer_id=customer_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def insert_verkauf_stueckbasiert(self, sales):
        sql = ('insert into STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF(VERKAUFS_ID, PRODUKT_ID, ANZAHL_PRODUKTE) '
               'values(:sale_id,:product_id,:product_count)')
        try:
            # create a cursor
            with self.con.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_verkauf_gewichtsbasiert(self, sales):
        sql = ('insert into GEWICHTBASIERTES_PRODUKT_IM_VERKAUF(VERKAUFS_ID, PRODUKT_ID, GEWICHT) '
               'values(:sale_id,:product_id,:weight)')
        try:
            # create a cursor
            with self.con.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_verkauf_row(self, sale_date, worker_id, customer_id, sale_sum, tax_sum, weight_sum):
        print("Verkauf: ", [sale_date, worker_id, customer_id, sale_sum, tax_sum, weight_sum])
        try:
            # create a cursor
            with self.con.cursor() as cursor:
                new_id = cursor.var(cx_Oracle.NUMBER)
                sql = (
                    "insert into VERKAUF(VERKAUFDATUM, MITARBEITER_ID, KUNDEN_ID, NETTOSUMME, STEUERSUMME, GESAMTGEWICHT)"
                    "values(to_date(:sale_date,'MM/DD/YYYY HH:MI AM'),:worker_id,:customer_id,:sale_sum,:tax_sum,:weight_sum)"
                    "returning VERKAUFS_ID into :7")
                # execute the insert statement
                cursor.execute(sql, [sale_date, worker_id, customer_id, sale_sum, tax_sum, weight_sum, new_id])
                added_id = new_id.getvalue()
                # commit work
                self.con.commit()
                return int(added_id[0])
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_einkauf_row(self):
        return

    def insert_einkauf_stueckbasiert(self, sales):
        sql = ('insert into STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF(EINKAUFS_ID, PRODUKT_ID, ANZAHL_PRODUKTE) '
               'values(:sale_id,:product_id,:product_count)')
        try:
            # create a cursor
            with self.con.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_einkauf_gewichtsbasiert(self, sales):
        sql = ('insert into GEWICHTBASIERTES_PRODUKT_IM_EINKAUF(EINKAUFS_ID, PRODUKT_ID, GEWICHT) '
               'values(:sale_id,:product_id,:weight)')
        try:
            # create a cursor
            with self.con.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def product_present(self, product_id):
        with self.con.cursor() as cursor:
            cursor.execute("""select * from PRODUKT WHERE PRODUKT_ID = :product_id""", product_id=product_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def insert_price(self, betrag, beginn, typ, produktid):
        sql = ("insert into PREIS(BETRAG, GUELTIGKEITS_BEGINN,  TYP, PRODUKT_ID) "
               "values(:betrag,to_date(:beginn,'MM/DD/YYYY HH:MI AM'), :typ, :produktid) ")
        try:
            # create a cursor
            with self.con.cursor() as cursor:
                cursor.execute(sql, [betrag, beginn, typ, produktid])
                self.con.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_product_id_by_name(self, productname: str):
        try:
            with self.con.cursor() as cursor:
                cursor.execute(f"select PRODUKT_ID from PRODUKT WHERE BEZEICHNUNG = :name", name=productname)
                rows = cursor.fetchall()
                if rows:
                    return rows[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    # def insert_dimension(self, pwidth: float, pheight: float, pdepth: float, pid: int):
    #     sql = ("update PRODUKT "
    #            "set PRODUKT_HOEHE=:pheight, PRODUKT_TIEFE= :pdepth , PRODUKT_BREITE =:pwidth "
    #            "where PRODUKT_ID = :pid")
    #     try:
    #         # create a cursor
    #         with self.con.cursor() as cursor:
    #             cursor.execute(sql, [pheight, pdepth, pwidth, pid])
    #             self.con.commit()
    #     except cx_Oracle.Error as error:
    #         print('Error occurred:')
    #         print(error)

    def insert_lagereinheit(self, nummer, zeile, spalte, breite, tiefe, hoehe, typ):
        sql = (
                "insert into LAGER_EINHEIT(REGAL_NUMMER, REGALZEILE,  REGALSPALTE, REGALBREITE, REGALTIEFE, REGALHÖHE, TYP) "
                "values(:regalnummer, :regalzeile, :regalspalte, :regalbreite, :regaltiefe,:regalhoehe, :typ) " + \
                "returning LAGERPLATZ_ID into :python_var")
        try:
            # create a cursor
            with self.con.cursor() as cursor:

                newest_id_wrapper = cursor.var(cx_Oracle.STRING)
                cursor.execute(sql, [nummer, zeile, spalte, breite, tiefe, hoehe, typ, newest_id_wrapper])
                newest_id = newest_id_wrapper.getvalue()
                # commit work
                self.con.commit()

                return int(newest_id[0])
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_products(self):
        try:
            with self.con.cursor() as cursor:

                cursor.execute(f"""select * from PRODUKT""")
                print("Fetching all products from database")
                rows = cursor.fetchall()
                if rows:
                    return rows
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_categoryid_from_productid(self, productid: int) -> int:
        try:
            with self.con.cursor() as cursor:
                # cursor.execute("""select * from ORTSKENNZAHL""")
                cursor.execute(
                    f"select PRODUKTKATEGORIE_ID from ZUWEISUNG_PRODUKT_PRODUKTKATEGORIE WHERE PRODUKT_ID = :id",
                    id=productid)
                rows = cursor.fetchall()
                if rows:
                    return rows[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_product_to_lagereinheit(self, productid: int, lagerid: int):
        sql = (
            "insert into ZUWEISUNG_PRODUKT_LAGERPLATZ(PRODUKT_ID, LAGERPLATZ_ID) "
            "values(:productid, :lagerid) ")
        try:
            # create a cursor
            with self.con.cursor() as cursor:
                cursor.execute(sql, [productid, lagerid])
                # commit work
                self.con.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_brand_id(self, brand_name: str):
        try:
            with self.con.cursor() as cursor:
                cursor.execute(f"select MARKE_ID from MARKE WHERE BEZEICHNUNG = :name", name=brand_name)
                rows = cursor.fetchall()
                return rows[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_product(self, nettogewicht: float, umsatzsteuer: float, bezeichnung: str, sku: int, art: str, marke: str,
                       bruttogewicht: float, pwidth: float, pheight: float, pdepth: float):
        try:
            sql = (
                'insert into PRODUKT(NETTOGEWICHT, UMSATZSTEUERSATZ, BEZEICHNUNG, SKU, TYP, MARKE_ID, BRUTTOGEWICHT, PRODUKT_HOEHE, PRODUKT_TIEFE , PRODUKT_BREITE) '  
                'values(:nettogewicht,:umsatzsteuer,:bezeichnung,:sku, :typ, :marke, :bruttogewicht, :pheight, :pdepth, :pwidth)')
            with self.con.cursor() as cursor:
                cursor.execute(sql, [nettogewicht, umsatzsteuer, bezeichnung, sku, art, marke, bruttogewicht, pwidth,
                                     pheight, pdepth])
                self.con.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)