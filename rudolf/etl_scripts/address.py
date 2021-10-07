from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.sqlite_service import SQLiteService
from rudolf.util import compare_strings


class Address:

    def __init__(self):
        self.con_f2 = F2DBService()
        self.con_master = CombDBService()
        self.con_rudolf = SQLiteService()
        self.f2_addresses_join: list[dict] = self.con_f2.select_all_addresses()

    def start(self):
        for address in self.f2_addresses_join:
            if not self._is_address_already_transfered(address.get("ADRESS_ID")):
                self._insert_addresses(address)

    # insert one address to database
    def _insert_addresses(self, address: dict):
        matching_addresses: list[dict] = self._get_matching_addresses_if_exists(address)
        if len(matching_addresses) == 1:
            combined_address_id = matching_addresses.__getitem__(0).get("ADRESS_ID")
            self.con_master.insert_address_datenherkunft(combined_address_id, 2)
            self.con_rudolf.insert_id_allocation(config.ADDRESS_DB_TABLE, combined_address_id, address.get("ADRESS_ID"))
        elif len(matching_addresses) == 0:
            # TODO manual nachfrage fuer land
            new_address_id: int = self.con_master.insert_address(land="Deutschland",
                                                                 plz=address.get("ORTSKENNZAHL"),
                                                                 ort=address.get("NAME"),
                                                                 strasse=address.get("STRASSE"),
                                                                 hausnummer=address.get("NUMMER"),
                                                                 bundesland=address.get("BEZEICHNUNG"))
            self.con_rudolf.insert_id_allocation(config.ADDRESS_DB_TABLE, new_address_id, address.get("ADRESS_ID"))

        else:
            # TODO manual auswahl zwischen eintraegen
            pass

    def _get_matching_addresses_if_exists(self, address_to_check: dict) -> list[dict]:
        return [address for address in self.f2_addresses_join
                if compare_strings(address.get("STRASSE"), address_to_check.get("STRASSE"))
                and compare_strings(address.get("ORTSKENNZAHL"), address_to_check.get("ORTSKENNZAHL"))
                and compare_strings(address.get("NUMMER"), address_to_check.get("NUMMER"))
                and compare_strings(address.get("NAME"), address_to_check.get("NAME"))]

    def _get_id_of_existing_address(self, address_to_check: dict) -> int:
        comb_address_id: int = next(address.get("ADRESS_ID") for address in self.f2_addresses_join
                                    if compare_strings(address.get("STRASSE"), address_to_check.get("STRASSE"))
                                    and compare_strings(address.get("ORTSKENNZAHL"),
                                                        address_to_check.get("ORTSKENNZAHL"))
                                    and compare_strings(address.get("NUMMER"), address_to_check.get("NUMMER"))
                                    and compare_strings(address.get("NAME"), address_to_check.get("NAME")))
        # TODO exception wenn id nicht vorhanden
        return comb_address_id

    def _is_address_already_transfered(self, old_address: int) -> bool:
        return len(self.con_rudolf.select_where_old_id(table_name=config.ADDRESS_DB_TABLE, old_id=old_address)) != 0


if __name__ == "__main__":
    # create object
    addressMerge = Address()
    addressMerge.start()
