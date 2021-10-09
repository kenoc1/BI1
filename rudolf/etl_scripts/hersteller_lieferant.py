from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService
from rudolf.util import compare_strings


class HerstellerLieferant:
    def __init__(self):
        self.db_f2 = F2DBService()
        self.db_master = CombDBService()
        self.db_rudolf = SQLiteService()
        self._get_data_basis()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_all_hersteller: list[dict] = self.db_f2.select_all_hersteller()
        self.comb_all_hersteller: list[dict] = self.db_master.select_all_lieferanten()

    def start(self):
        for hersteller in self.f2_all_hersteller:
            # TODO try/catch
            if not self._is_hersteller_already_transferred(hersteller.get("HERSTELLER_ID")):
                self._insert_hersteller_into_lieferant(hersteller)

    def _insert_hersteller_into_lieferant(self, hersteller: dict):
        print(hersteller)
        address_id = config.DUMMY_ADDRESS
        hersteller_name = hersteller["BEZEICHNUNG"]
        mail = config.DUMMY_MAIL
        matching_hersteller: list[dict] = self._get_matching_hersteller_if_exists(hersteller_name)
        if len(matching_hersteller) == 1:
            combined_hersteller_id = matching_hersteller.__getitem__(0).get("LIEFERANT_ID")
            self.db_master.insert_lieferant_datenherkunft(combined_hersteller_id, config.SOURCE_F2)
            self.db_rudolf.insert_id_allocation(config.LIEFERANT_HERSTELLER_TABLE, combined_hersteller_id,
                                                hersteller.get("HERSTELLER_ID"))
        elif len(matching_hersteller) == 0:
            new_hersteller_id: int = self.db_master.insert_lieferant(address_id, hersteller_name, mail)
            self.db_master.insert_lieferant_datenherkunft(new_hersteller_id, config.SOURCE_F2)
            self.db_rudolf.insert_id_allocation(config.LIEFERANT_HERSTELLER_TABLE, new_hersteller_id,
                                                hersteller.get("HERSTELLER_ID"))
        else:
            # TODO manual check
            pass

    def _get_matching_hersteller_if_exists(self, f2_supplier_name: str):
        return [comb_supplier for comb_supplier in self.comb_all_hersteller
                if compare_strings(comb_supplier.get("LIEFERANT_NAME"), f2_supplier_name)]

    def _is_hersteller_already_transferred(self, f2_hersteller_id: int) -> bool:
        try:
            self.db_rudolf.select_where_old_id(config.LIEFERANT_HERSTELLER_TABLE, f2_hersteller_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False


if __name__ == "__main__":
    pass
    # testing
    # Supplier()

    # prod
    # Supplier().insert_hersteller_into_lieferant()
    # Supplier().supplier_present_check_with_description()
