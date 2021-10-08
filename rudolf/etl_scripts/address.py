from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService
from rudolf.util import compare_strings


class Address:

    def __init__(self):
        self.con_f2 = F2DBService()
        self.con_master = CombDBService()
        self.con_rudolf = SQLiteService()
        self._get_data_basis()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_addresses_join: list[dict] = self.con_f2.select_all_addresses_join()
        self.comb_addresses: list[dict] = self.con_master.select_all_addresses()

    def start(self):
        for address in self.f2_addresses_join:
            # TODO try/catch
            if not self._is_address_already_transferred(address.get("ADRESS_ID")):
                self._insert_addresses(address)

    def _insert_addresses(self, address: dict):
        matching_addresses: list[dict] = self._get_matching_addresses_if_exists(address)
        if len(matching_addresses) == 1:
            combined_address_id = matching_addresses.__getitem__(0).get("ADRESS_ID")
            self.con_master.insert_address_datenherkunft(combined_address_id, config.SOURCE_F2)
            self.con_rudolf.insert_id_allocation(config.ADDRESS_DB_TABLE, combined_address_id, address.get("ADRESS_ID"))
        elif len(matching_addresses) == 0:
            # TODO manual nachfrage fuer land
            new_address_id: int = self.con_master.insert_address(land="Deutschland",
                                                                 plz=address.get("ORTSKENNZAHL"),
                                                                 ort=address.get("NAME"),
                                                                 strasse=address.get("STRASSE"),
                                                                 hausnummer=address.get("NUMMER"),
                                                                 bundesland=address.get("BEZEICHNUNG"))
            self.con_master.insert_address_datenherkunft(new_address_id, config.SOURCE_F2)
            self.con_rudolf.insert_id_allocation(config.ADDRESS_DB_TABLE, new_address_id, address.get("ADRESS_ID"))
        else:
            # TODO manual auswahl zwischen eintraegen
            pass

    def _get_matching_addresses_if_exists(self, f2_address: dict) -> list[dict]:
        return [comb_address for comb_address in self.comb_addresses
                if compare_strings(comb_address.get("STRASSE"), f2_address.get("STRASSE"))
                and compare_strings(comb_address.get("ORTSKENNZAHL"), f2_address.get("ORTSKENNZAHL"))
                and compare_strings(comb_address.get("NUMMER"), f2_address.get("NUMMER"))
                and compare_strings(comb_address.get("NAME"), f2_address.get("NAME"))]

    def _is_address_already_transferred(self, old_address: int) -> bool:
        try:
            self.con_rudolf.select_where_old_id(table_name=config.ADDRESS_DB_TABLE, old_id=old_address)
            return True
        except NoCombIDFoundForF2IDException:
            return False


if __name__ == "__main__":
    # create object
    addressMerge = Address()
    addressMerge.start()
