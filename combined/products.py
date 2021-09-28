from db_service import DB_F2, DB_MASTER
import key_allocation_saver
import key_allocation_reader
import util
import config


# --- OS PRODUKT---
# PRODUKT_ID NUMBER primary key,
# LIEFERANT_ID NUMBER
# PRODUKTKLASSE_ID NUMBER
# PROUKT_NAME VARCHAR2(150) not null,
# EAN VARCHAR2(50) not null,
# SKU VARCHAR2(50) not null,
# PRODUKTBESCHREIBUNG VARCHAR2(500),
# ANGEBOTSRABATT FLOAT not null check (angebotsrabatt between 0 and 1),
# EINHEITSGROESSE VARCHAR2(30),
# EINKAUFSPREIS FLOAT not null,
# LISTENVERKAUFSPREIS FLOAT not null,
# RANKING FLOAT,
# RECYCLEBAR NUMBER(1),
# LOW_FAT NUMBER(1),
# BEWERTUNG FLOAT,
# MWST_SATZ FLOAT not null	check (mwst_satz between 0 and 1)


# --- F2 PRODUKT ---
# Produkt_Id       NUMBER(10),
# Gewicht          NUMBER(10, 4) CHECK (Gewicht >= '0'),
# Umsatzsteuersatz NUMBER(10, 2)      NOT NULL CHECK (Umsatzsteuersatz >= '0'),
# Bezeichnung      VARCHAR2(128 CHAR) NOT NULL,
# SKU              NUMBER(10)         NOT NULL CHECK (SKU >= '0'),
# Typ              VARCHAR2(15 CHAR) CHECK ( Typ IN ('gewichtsbasiert', 'stueckbasiert')),
# Produkt_Hoehe    NUMBER(10, 2),
# Produkt_Tiefe    NUMBER(10, 2),
# Produkt_Breite   NUMBER(10, 2),
# Marke_Id         NUMBER(10)         NOT NULL, FK


# --- F2 PREIS ---
# PREIS_ID                NUMBER(10) primary key,
# BETRAG                  NUMBER(10, 2)
# GUELTIGKEITS_BEGINN     DATE
# GUELTIGKEITS_ENDE       DATE,
# TYP                     VARCHAR2(15 char),
# PRODUKT_ID              NUMBER(10), FK


# --- MASTER PREISHISTORIE ---
# PREISHISTORIE_ID NUMBER primary key,
# PRODUKT_ID NUMBER
# constraint FK_PRODUKT_PREISHISTORIE_ID references PRODUKT,
# BETRAG NUMBER,
# START_TIMESTAMP DATE,
# END_TIMESTAMP DATE,
# TYP VARCHAR2(15 CHAR) CHECK ( Typ IN ('EINKAUFSPREIS', 'LISTENVERKAUFSPREIS'))

class Products:
    def __init__(self):
        self.db_f2 = DB_F2()
        self.db_master = DB_MASTER()
        self.con_cat = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(config.PRODUCT_CAT_CON_FILE_NAME)
        # testing
        # print(self.db_master.product_present_check_with_sku("22576443552", 19))

    @staticmethod
    def _convert_mwst(mwst: float):
        return mwst * 0.01

    @staticmethod
    def _get_new_product_class_id(f2_class_id: str):
        # Informationen werden hier angereichert
        # ToDo: get id out of CSV
        return 0

    def insert_products_from_f2_to_master(self):
        f2_master_products_connection = []
        products = self.db_f2.select_all_produkte()
        for product in products:
            print(product)
            product_id_f2 = product["PRODUKT_ID"]
            supplier_id = self.db_f2.get_supplier_id_with_brand_id(product["MARKE_ID"])
            product_class_id = self._get_new_product_class_id(
                self.db_f2.select_categoryid_from_productid(product_id_f2))
            product_name = product["BEZEICHNUNG"]
            sku = product["SKU"]
            discount = 0
            # discount = config.DUMMY_DISCOUNT

            # ToDo: Was passiert, wenn sich Marken aendern?
            # Marken als Attribut
            brand_name = self.db_f2.get_brand_name(product["MARKE_ID"])

            # Preis umrechnen Einheit
            purchasing_price = util.ib_dollar_to_euro(
                self.db_f2.select_current_buying_price_with_product_id(product_id_f2)["BETRAG"])
            selling_price = util.ib_dollar_to_euro(
                self.db_f2.select_current_sale_price_with_product_id(product_id_f2)["BETRAG"])
            mwst = self._convert_mwst(float(product["UMSATZSTEUERSATZ"]))

            if product["TYP"] == config.PRODUCT_TYP_F2[0]:
                weight = util.ib_lbs_to_kg(product["NETTOGEWICHT"])
                purchasing_price = (1 / weight) * purchasing_price
                selling_price = (1 / weight) * selling_price

                size_fit = config.GEWICHTSBASIERT_EINHEIT_STUECK
            else:
                size_fit = 1

            product_present_id = self.db_master.product_present_check_with_sku(sku, supplier_id)

            if not product_present_id:
                # product_present_id = self.db_master.insert_product_row_only_required(supplier_id, product_class_id, product_name,
                #                                                          sku, discount, size_fit,
                #                                                          purchasing_price, selling_price, mwst, brand_name)
                product_present_id = 1
                f2_master_products_connection.append([product_present_id, product_id_f2])

            if not self.db_master.source_present_check_product(product_present_id):
                self.db_master.insert_source_product()

        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(f2_master_products_connection,
                                                                   config.PRODUCTS_CON_FILE_NAME)

    def insert_product_price_history(self):
        prices = self.db_f2.select_all_preise()
        for price in prices:
            print(price)
            product_id = price["PRODUKT_ID"]
            price = price["BETRAG"]
            if price["TYP"] == config.PREIS_TYP_F2[0]:
                typ = config.PREIS_TYP[0]
            else:
                typ = config.PREIS_TYP[1]
            start_date = price["GUELTIGKEITS_BEGINN"]
            self.db_master.insert_product_price_history(product_id=product_id, price=price, typ=typ,
                                                        start_date=start_date)

# testing
# Products()

# prod
# products = Products()
# products.insert_products_from_f2_to_master()
# products.insert_product_price_history()
