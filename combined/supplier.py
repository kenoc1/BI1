import config
import util
from db_service import DB_F2, DB_MASTER
import key_allocation_saver
import string_equality_tester


# --- OS Lieferant---
# LIEFERANT_ID NUMBER
# ADRESSE_ID NUMBER
# LIEFERANT_NAME VARCHAR2(255) not null,
# EMAIL VARCHAR2(100) not null,
# TELEFONNUMMER VARCHAR2(50),
# URL VARCHAR2(150),
# KONTAKT_NACHNAME VARCHAR2(50),
# KONTAKT_VORNAME VARCHAR2(50),
# RANKING NUMBER,
# IBAN VARCHAR2(50)


# --- F2 Hersteller ---
# HERSTELLER_ID NUMBER
# BEZEICHNUNG VARCHAR2(255)

class Supplier:
    def __init__(self):
        self.db_f2 = DB_F2()
        self.db_master = DB_MASTER()
        # testing
        # self.db_master.supplier_present_check_with_description("Schropp und Merten AG")

    def insert_hersteller_to_lieferanten(self):
        f2_master_supplier_connection = []
        hersteller = self.db_f2.select_all_hersteller()
        for item in hersteller:
            print(item)
            address_id = config.DUMMY_ADDRESS
            supplier_name = item["BEZEICHNUNG"]
            mail = config.DUMMY_MAIL
            if not self.db_master.supplier_present_check_with_description(supplier_name):
                new_id = self.db_master.insert_lieferant_row_only_required(address_id, supplier_name, mail)
                f2_master_supplier_connection.append([new_id, item['HERSTELLER_ID']])
                # ToDo: Herkunft
            else:
                pass
                # ToDO: Herkunft prüfen und ggf. hinzufügen
        if f2_master_supplier_connection:
            key_allocation_saver.save_f2_to_comb_id_allocation_to_file(f2_master_supplier_connection,
                                                                       "f2_master_lieferant_hersteller_con.csv")


# testing
Supplier()

# prod
# Supplier().insert_hersteller_to_lieferanten()
