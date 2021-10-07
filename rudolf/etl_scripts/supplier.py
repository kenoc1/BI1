from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService


class Supplier:
    def __init__(self):
        self.db_f2 = F2DBService()
        self.db_master = CombDBService()
        # self.db_master.supplier_present_check_with_description("Schropp und Merten AG")

    def insert_hersteller_into_lieferant(self):
        f2_master_supplier_connection = []
        hersteller = self.db_f2.select_all_hersteller()
        for item in hersteller:
            print(item)
            address_id = config.DUMMY_ADDRESS
            supplier_name = item["BEZEICHNUNG"]
            mail = config.DUMMY_MAIL
            supplier_present_id = self.db_master.supplier_present_check_with_description(supplier_name)

            if not supplier_present_id:
                supplier_present_id = self.db_master.insert_lieferant_row_only_required(address_id, supplier_name, mail)
                print(address_id, supplier_name, mail)
                f2_master_supplier_connection.append([supplier_present_id, item['HERSTELLER_ID']])
            if not self.db_master.source_present_check_supplier(supplier_present_id, config.SOURCE_F2):
                self.db_master.insert_source_supplier(supplier_present_id, config.SOURCE_F2)
                print(supplier_present_id, config.SOURCE_F2)

        if f2_master_supplier_connection:
            key_allocation_saver.write_to_csv(rows=f2_master_supplier_connection,
                                              header=[],
                                              filepath=config.SUPPLIER_CON_FILE_NAME)


if __name__ == "__main__":
    # testing
    # Supplier()

    # prod
    Supplier().insert_hersteller_into_lieferant()
