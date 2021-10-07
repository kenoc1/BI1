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
        self.con_cat = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(config.PRODUCT_SUB_CAT_CON_FILE_NAME)
        self.con_sup = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(config.SUPPLIER_CON_FILE_NAME)
        self.con_brand = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(config.BRAND_CON_FILE_NAME)
        self.con_products = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(config.PRODUCTS_CON_FILE_NAME)
        # testing
        # print(self.db_master.product_present_check_with_sku("22576443552", 19))
        # print(self._get_new_supplier_id(33))
        # print(self._get_new_product_class_id(1))

    @staticmethod
    def _convert_mwst(mwst: float):
        return mwst * 0.01

    def _get_new_product_class_id(self, f2_class_id: str):
        # Informationen werden hier angereichert
        return util.search_for_id(self.con_cat, f2_class_id)

    def _get_new_supplier_id(self, f2_supplier_id: str):
        return util.search_for_id(self.con_sup, f2_supplier_id)

    def _get_new_brand_id(self, f2_brand_id: str):
        return util.search_for_id(self.con_brand, f2_brand_id)

    def _get_new_product_id(self, f2_product_id: str):
        return util.search_for_id(self.con_products, f2_product_id)

    def insert_products_from_f2_to_master(self):
        f2_master_products_connection = []
        products = self.db_f2.select_all_produkte()
        for product in products:
            print(product)
            product_id_f2 = product["PRODUKT_ID"]
            supplier_id = self._get_new_supplier_id(self.db_f2.get_supplier_id_with_brand_id(product["MARKE_ID"]))
            product_class_id = self._get_new_product_class_id(
                self.db_f2.select_categoryid_from_productid(product_id_f2))
            product_name = product["BEZEICHNUNG"]
            sku = product["SKU"]
            discount = 0
            ean = config.DUMMY_EAN

            # Marken als Enität hinzufuegen
            brand_id = self._get_new_brand_id(product["MARKE_ID"])

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
                product_present_id = self.db_master.insert_product_row_only_required(supplier_id=supplier_id,
                                                                                     product_class_id=product_class_id,
                                                                                     product_name=product_name,
                                                                                     sku=sku, ean=ean,
                                                                                     discount=discount,
                                                                                     size_fit=size_fit,
                                                                                     purchasing_price=purchasing_price,
                                                                                     selling_price=selling_price,
                                                                                     mwst=mwst, brand_id=brand_id,
                                                                                     source_system=config.SOURCE_F2)
                if product_present_id:
                    f2_master_products_connection.append([product_present_id, product_id_f2])

            # if not self.db_master.source_present_check_product(product_id=product_present_id, source_id=config.SOURCE_F2):
            #     self.db_master.insert_source_product(product_present_id, config.SOURCE_F2)

        key_allocation_saver.write_to_csv(rows=f2_master_products_connection,
                                          filepath=config.PRODUCTS_CON_FILE_NAME)

    def insert_product_price_history(self):
        prices = self.db_f2.select_all_preise()
        for price_item in prices:
            print(price_item)
            f2_product_id = price_item["PRODUKT_ID"]
            product_id = self._get_new_product_id(f2_product_id)
            price = price_item["BETRAG"]
            if price_item["TYP"] == config.PREIS_TYP_F2[0]:
                typ = config.PREIS_TYP[0]
            else:
                typ = config.PREIS_TYP[1]
            start_date = price_item["GUELTIGKEITS_BEGINN"]
            if not self.db_master.price_present(product_id=product_id, price=price, typ=typ,
                                                start_date=start_date):
                self.db_master.insert_product_price_history(product_id=product_id, price=price, typ=typ,
                                                            start_date=start_date)

    def insert_brands(self):
        f2_master_brands_connection = []
        brands = self.db_f2.select_all_marken()
        for brand in brands:
            print(brand)
            supplier_id = self._get_new_supplier_id(brand["HERSTELLER_ID"])
            brand_name = brand["BEZEICHNUNG"]
            if not self.db_master.brand_present(supplier_id=supplier_id, brand_name=brand_name):
                new_id = self.db_master.insert_brand_row(supplier_id=supplier_id, brand_name=brand_name)
                f2_master_brands_connection.append([new_id, brand["MARKE_ID"]])
        key_allocation_saver.write_to_csv(rows=f2_master_brands_connection,
                                          filepath=config.BRAND_CON_FILE_NAME)


if __name__ == "__main__":
    # testing
    # Products()

    # prod
    products = Products()
    # products.insert_brands()
    # products.insert_products_from_f2_to_master()
    # products.insert_product_price_history()
