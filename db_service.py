from collections import Counter
from random import randint

import cx_Oracle

import config


class DB_F2:
    def __init__(self):
        self.con_f2 = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2,
                                        dsn=config.DB_CON_DSN_F2,
                                        encoding="UTF-8")
        print("Database version:", self.con_f2.version)

    # selects
    def select_table(self, table_name: str):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"""select * from {table_name}""")
                rows = cursor.fetchall()
                if rows:
                    return rows
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_all_customers(self):
        return self._select_all_dict("KUNDE")

    def select_all_workers(self):
        return self._select_all_dict("MITARBEITER")

    def select_all_delivery_notes(self):
        return self._select_all_dict("LIEFERSCHEIN")

    def select_all_invoices(self):
        return self._select_all_dict("RECHNUNG")

    def select_all_bons(self):
        return self._select_all_dict("BON")

    def select_all_worker_functions(self):
        return self._select_all_dict("FUNKTION")

    def select_all_produktoberkategorien(self):
        return self._select_all_dict("PRODUKTOBERKATEGORIE")

    def select_all_produktkategorien(self):
        return self._select_all_dict("PRODUKTKATEGORIE")

    def _select_all_dict(self, table_name):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"""select * from {table_name}""")
                cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
                rows = cursor.fetchall()
                if rows:
                    return rows
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_product_with_description(self, description):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"""SELECT * FROM PRODUKT WHERE BEZEICHNUNG Like :description""",
                               description=f"{description}%")
                cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
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
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"""select * from {config.PRODUCTS_F2} where PRODUKT_ID = :product_id""",
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

    def select_current_sale_price_with_product_id(self, product_id):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select * from PREIS where PRODUKT_ID = :product_id and TYP = 'Verkauf' and GUELTIGKEITS_ENDE is null""",
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

    def select_current_buying_price_with_product_id(self, product_id):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(
                    f"""select * from PREIS where PRODUKT_ID = :product_id and TYP = 'Einkauf' and GUELTIGKEITS_ENDE is null""",
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

    def get_id_from_function(self, function_description):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select * from FUNKTION WHERE BEZEICHNUNG = :function_description""",
                               function_description=function_description)
                row = cursor.fetchone()
            if row:
                return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    # F2

    def insert_mitarbeiter_row(self, first_name, last_name, commission_rate, salary, address_id):
        sql = ('insert into MITARBEITER(VORNAME, NACHNAME, PROVISIONSSATZ, GEHALT, ADRESS_ID)'
               'values(:first_name,:last_name,:commission_rate,:salary,:address_id)')
        print("Mitarbeiter: ", [first_name, last_name, commission_rate, salary, address_id])
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                # execute the insert statement
                cursor.execute(sql, [first_name, last_name, commission_rate, salary, address_id])
                # commit work
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_kunde_row(self, first_name, last_name, birthdate, billing_address, shipping_address):
        sql = ("insert into KUNDE(VORNAME, NACHNAME, GEBURTSDATUM, RECHNUNGS_ADRESSE_ID, LIEFER_ADRESSE_ID)"
               "values(:first_name,:last_name, to_date(:birthdate,'MM/DD/YYYY HH:MI AM'),:billing_address,:shipping_address)")
        print("Kunden: ", [first_name, last_name, birthdate, billing_address, shipping_address])
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                # execute the insert statement
                cursor.execute(sql, [first_name, last_name, birthdate, billing_address, shipping_address])
                # commit work
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_sale_documents(self):

        return

    def insert_bon_row(self, sale_id, given_money, payment_method, sale_sum):
        try:
            if payment_method == "Bar":
                sql = ('insert into BON(VERKAUFS_ID, GEGEBENES_GELD, ZAHLUNGSART, RUECKGELD) '
                       'values(:sale_id,:given_money,:payment_method,:change)')
                # create a cursor
                with self.con_f2.cursor() as cursor:
                    cursor.execute(sql, [sale_id, given_money, payment_method, (given_money - sale_sum)])
                    self.con_f2.commit()
                return given_money - sale_sum
            else:
                sql = ('insert into BON(VERKAUFS_ID, ZAHLUNGSART) '
                       'values(:sale_id,:payment_method)')
                # create a cursor
                with self.con_f2.cursor() as cursor:
                    cursor.execute(sql, [sale_id, payment_method])
                    self.con_f2.commit()
                return 0
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_invoice_row(self, sale_id, adjustment_date):
        sql = ('insert into RECHNUNG(VERKAUFS_ID, ABGLEICH_DATUM) '
               "values(:sale_id,to_date(:adjustment_date,'MM/DD/YYYY HH:MI AM'))")
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [sale_id, adjustment_date])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_delivery_note_row(self, sale_id, delivery_date, delivery_costs):
        sql = ("insert into LIEFERSCHEIN(LIEFERDATUM, VERKAUFS_ID, LIEFER_KOSTEN)"
               "values(to_date(:delivery_date,'MM/DD/YYYY HH:MI AM'),:sale_id,:delivery_costs)")
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [delivery_date, sale_id, delivery_costs])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_verkauf_stueckbasiert(self, sales):
        sql = ('insert into STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF(VERKAUFS_ID, PRODUKT_ID, ANZAHL_PRODUKTE) '
               'values(:sale_id,:product_id,:product_count)')
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_verkauf_gewichtsbasiert(self, sales):
        sql = ('insert into GEWICHTBASIERTES_PRODUKT_IM_VERKAUF(VERKAUFS_ID, PRODUKT_ID, GEWICHT) '
               'values(:sale_id,:product_id,:weight)')
        # try:
        #     # create a cursor
        #     with self.con_f2.cursor() as cursor:
        #         cursor.execute(sql, [50704, 2913, 10])
        #         self.con_f2.commit()
        # except cx_Oracle.Error as error:
        #     print('Error occurred:')
        #     print(error)
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_verkauf_row(self, sale_date, worker_id, customer_id, tax_sum, sale_netto_sum, sale_brutto_sum,
                           weight_sum):
        print("Verkauf: ", [sale_date, worker_id, customer_id, tax_sum, sale_netto_sum, sale_brutto_sum, weight_sum])
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                new_id = cursor.var(cx_Oracle.NUMBER)
                sql = (
                    "insert into VERKAUF(VERKAUFDATUM, MITARBEITER_ID, KUNDEN_ID, UST_SUMME, NETTO_SUMME, BRUTTO_SUMME, GESAMTGEWICHT)"
                    "values(to_date(:sale_date,'MM/DD/YYYY HH:MI AM'),:worker_id,:customer_id,:tax_sum,:sale_netto_sum,:sale_brutto_sum,:weight_sum)"
                    "returning VERKAUFS_ID into :8")
                # execute the insert statement
                cursor.execute(sql,
                               [sale_date, worker_id, customer_id, tax_sum, sale_netto_sum, sale_brutto_sum, weight_sum,
                                new_id])
                added_id = new_id.getvalue()
                # commit work
                self.con_f2.commit()
                return int(added_id[0])
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_einkauf_row(self, buying_date, worker_id, supplier_id):
        print("Einkauf: ", [buying_date, worker_id, supplier_id])
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                new_id = cursor.var(cx_Oracle.NUMBER)
                sql = (
                    "insert into EINKAUF(EINKAUFSDATUM, MITARBEITER_ID, LIEFERANT_ID)"
                    "values(to_date(:sale_date,'MM/DD/YYYY HH:MI AM'),:worker_id,:supplier_id)"
                    "returning EINKAUFS_ID into :4")
                # execute the insert statement
                cursor.execute(sql,
                               [buying_date, worker_id, supplier_id, new_id])
                added_id = new_id.getvalue()
                # commit work
                self.con_f2.commit()
                return int(added_id[0])
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_einkauf_stueckbasiert(self, sales):
        sql = ('insert into STUECKZAHLBASIERTES_PRODUKT_IM_EINKAUF(EINKAUFS_ID, PRODUKT_ID, ANZAHL_PRODUKTE) '
               'values(:sale_id,:product_id,:product_count)')
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_einkauf_gewichtsbasiert(self, sales):
        sql = ('insert into GEWICHTBASIERTES_PRODUKT_IM_EINKAUF(EINKAUFS_ID, PRODUKT_ID, GEWICHT) '
               'values(:sale_id,:product_id,:weight)')
        try:
            # create a cursor
            with self.con_f2.cursor() as cursor:
                cursor.executemany(sql, sales)
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def calculate_and_insert_count_products_verkauf(self, sale_id, count_products):
        count_products_ids = []
        if count_products:
            tmp_product_ids = []
            for product in count_products:
                tmp_product_ids.append(product["PRODUKT_ID"])
            tmp_product_ids_counter = Counter(tmp_product_ids)
            for product_id in tmp_product_ids_counter:
                count_products_ids.append(
                    [sale_id, product_id, tmp_product_ids_counter[product_id]])
            self.insert_verkauf_stueckbasiert(count_products_ids)

    def calculate_and_insert_weight_products_verkauf(self, sale_id, weight_products):
        weight_products_ids = []
        if weight_products:
            for product in weight_products:
                if any(product["PRODUKT_ID"] in sublist for sublist in weight_products_ids):
                    for item in weight_products_ids:
                        if item[1] == product["PRODUKT_ID"]:
                            item[2] += product["PURCHASED_WEIGHT"]
                else:
                    weight_products_ids.append(
                        [sale_id, product["PRODUKT_ID"], product["PURCHASED_WEIGHT"]])
            self.insert_verkauf_gewichtsbasiert(weight_products_ids)

    def calculate_and_insert_count_products_einkauf(self, sale_id, count_products):
        count_products_ids = []
        if count_products:
            tmp_product_ids = []
            for product in count_products:
                tmp_product_ids.append(product["PRODUKT_ID"])
            tmp_product_ids_counter = Counter(tmp_product_ids)
            for product_id in tmp_product_ids_counter:
                count_products_ids.append(
                    [sale_id, product_id, tmp_product_ids_counter[product_id]])
            self.insert_einkauf_stueckbasiert(count_products_ids)

    def calculate_and_insert_weight_products_einkauf(self, sale_id, weight_products):
        weight_products_ids = []
        if weight_products:
            for product in weight_products:
                if any(product["PRODUKT_ID"] in sublist for sublist in weight_products_ids):
                    for item in weight_products_ids:
                        if item[1] == product["PRODUKT_ID"]:
                            item[2] += product["PURCHASED_WEIGHT"]
                else:
                    weight_products_ids.append([sale_id,
                                                product["PRODUKT_ID"],
                                                product["PURCHASED_WEIGHT"]])
            self.insert_einkauf_gewichtsbasiert(weight_products_ids)

    # start - end - IDs ----------------------------------------------------------------------------------------------
    def get_start_customer_id(self):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select MIN(KUNDEN_ID) from KUNDE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_end_customer_id(self):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select MAX(KUNDEN_ID) from KUNDE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_address_id_min(self) -> int:
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select MIN(ADRESS_ID) from ADRESSE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_address_id_max(self) -> int:
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select MAX(ADRESS_ID) from ADRESSE""")
                row = cursor.fetchone()
                if row:
                    return row[0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_random_seller(self):
        try:
            with self.con_f2.cursor() as cursor:
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
            with self.con_f2.cursor() as cursor:
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
        return customer_id

    def get_random_supplier(self):
        start_supplier_id = self.get_start_supplier_id()
        end_supplier_id = self.get_end_supplier_id()
        present_supplier = False
        while not present_supplier:
            supplier_id = randint(start_supplier_id, end_supplier_id)
            present_supplier = self.supplier_present(supplier_id)
        return supplier_id

    def get_start_supplier_id(self):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select MIN(LIEFERANT_ID) from LIEFERANT""")
                row = cursor.fetchone()
                if row:
                    return row[0]
                else:
                    raise cx_Oracle.Error("no value found")
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_end_supplier_id(self):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute("""select MAX(LIEFERANT_ID) from LIEFERANT""")
                row = cursor.fetchone()
                if row:
                    return row[0]
                else:
                    raise cx_Oracle.Error("no value found")
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    # checks for present items ---------------------------------------------------------------------------------------
    def product_present(self, product_id):
        with self.con_f2.cursor() as cursor:
            cursor.execute("""select * from PRODUKT WHERE PRODUKT_ID = :product_id""", product_id=product_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def worker_present(self, worker_id):
        with self.con_f2.cursor() as cursor:
            cursor.execute("""select * from MITARBEITER WHERE MITARBEITER_ID = :worker_id""", worker_id=worker_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def address_present(self, address_id):
        with self.con_f2.cursor() as cursor:
            cursor.execute("""select * from ADRESSE WHERE ADRESS_ID = :address_id""", address_id=address_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def customer_present(self, customer_id):
        with self.con_f2.cursor() as cursor:
            cursor.execute("""select * from KUNDE WHERE KUNDEN_ID = :customer_id""", customer_id=customer_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def supplier_present(self, supplier_id):
        with self.con_f2.cursor() as cursor:
            cursor.execute("""select * from LIEFERANT WHERE LIEFERANT_ID = :supplier_id""", supplier_id=supplier_id)
            row = cursor.fetchone()
            if row:
                return True
            else:
                False

    def product_present(self, product_id):
        with self.con_f2.cursor() as cursor:
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
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [betrag, beginn, typ, produktid])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_product_id_by_name(self, productname: str):
        try:
            with self.con_f2.cursor() as cursor:
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
    #         with self.con_f2.cursor() as cursor:
    #             cursor.execute(sql, [pheight, pdepth, pwidth, pid])
    #             self.con_f2.commit()
    #     except cx_Oracle.Error as error:
    #         print('Error occurred:')
    #         print(error)

    def insert_lagereinheit(self, nummer, zeile, spalte, breite, tiefe, hoehe, typ):
        sql = (
                "insert into LAGER_EINHEIT(REGAL_NUMMER, REGALZEILE,  REGALSPALTE, REGALBREITE, REGALTIEFE, REGALHÖHE, TYP) "
                "values(:regalnummer, :regalzeile, :regalspalte, :regalbreite, :regaltiefe,:regalhoehe, :typ) " + \
                "returning LAGERPLATZ_ID into :python_var")
        try:
            with self.con_f2.cursor() as cursor:
                newest_id_wrapper = cursor.var(cx_Oracle.STRING)
                cursor.execute(sql, [nummer, zeile, spalte, breite, tiefe, hoehe, typ, newest_id_wrapper])
                newest_id = newest_id_wrapper.getvalue()
                self.con_f2.commit()
                return int(newest_id[0])
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_products(self):
        try:
            with self.con_f2.cursor() as cursor:
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
            with self.con_f2.cursor() as cursor:
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
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [productid, lagerid])
                # commit work
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_brand_id(self, brand_name: str):
        try:
            with self.con_f2.cursor() as cursor:
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
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [nettogewicht, umsatzsteuer, bezeichnung, sku, art, marke, bruttogewicht, pwidth,
                                     pheight, pdepth])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_subcategory(self, description: str, age_restriction: int):
        try:
            sql = (
                'insert into PRODUKTKATEGORIE(BEZEICHNUNG, ALTERFREIGABE) '
                'values(:bezeichnung, :alters)')
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [description, age_restriction])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_product_subcategory(self, product_id: int, cat_id: int):
        try:
            sql = (
                'insert into ZUWEISUNG_PRODUKT_PRODUKTKATEGORIE(PRODUKT_ID, PRODUKTKATEGORIE_ID) '
                'values(:productid, :catid)')
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [product_id, cat_id])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_product_id_by_name(self, product_name: str):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"select PRODUKT_ID from PRODUKT WHERE BEZEICHNUNG = :name", name=product_name)
                rows = cursor.fetchall()
                if rows:
                    return rows[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def get_category_id_by_name(self, category_name: str):
        try:
            with self.con_f2.cursor() as cursor:
                cursor.execute(f"select PRODUKTKATEGORIE_ID from PRODUKTKATEGORIE WHERE BEZEICHNUNG = :name",
                               name=category_name)
                rows = cursor.fetchall()
                if rows:
                    return rows[0][0]
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def insert_oberkategorie_subcategory(self, subcat_id: int, ocat_id: int):
        try:
            sql = (
                'insert into ZUWEISUNG_KATEGORIE_OBERKATEGORIE(PRODUKTKATEGORIE_ID, PRODUKTOBERKATEGORIE_ID) '
                'values(:catid, :ocatid)')
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [subcat_id, ocat_id])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_all_oberkategorie(self, subcat_id: int, ocat_id: int):
        try:
            sql = (
                'select  ZUWEISUNG_KATEGORIE_OBERKATEGORIE(PRODUKTKATEGORIE_ID, PRODUKTOBERKATEGORIE_ID) '
                'values(:catid, :ocatid)')
            with self.con_f2.cursor() as cursor:
                cursor.execute(sql, [subcat_id, ocat_id])
                self.con_f2.commit()
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)


class DB_MASTER:
    def __init__(self):
        self.con_master = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                                            dsn=config.DB_CON_DSN_COMBINED, encoding="UTF-8")
        print("Database version:", self.con_master.version)

    def select_table(self, table_name: str) -> list:
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute(f"""select * from {table_name}""")
                rows = cursor.fetchall()
                if rows:
                    return rows
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def _select_all_dict(self, table_name: str) -> list:
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute(f"""select * from {table_name}""")
                cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
                rows = cursor.fetchall()
                if rows:
                    return rows
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

    def select_subcat_where_bezeichnung(self, bezeichnung: str) -> list:
        try:
            with self.con_master.cursor() as cursor:
                cursor.execute(f"""select * from PRODUKT_SUBKATEGORIE WHERE BEZEICHNUNG = :bezeichnung""",
                               bezeichnung=bezeichnung)
                cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
                rows = cursor.fetchall()
                if rows:
                    return rows
                else:
                    return []
        except cx_Oracle.Error as error:
            print('Error occurred:')
            print(error)

# class DB_OS:
#     def __init__(self):
#         return
