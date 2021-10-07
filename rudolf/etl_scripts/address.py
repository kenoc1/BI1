import cx_Oracle

from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.util import compare_strings


class Address:

    def __init__(self):
        self.con_f2 = F2DBService()
        self.con_master = CombDBService()
        self.f2_addresses_join: list[dict] = self.con_f2.select_all_addresses()

    def start(self):
        array_for_csv = []

        for address in self.f2_addresses_join:
            self.insert_addresses(address, array_for_csv)

        # TODO allcoation in db schreiben

        # create csv for allocation
        key_allocation_saver.save_f2_to_comb_id_allocation_to_file(array_for_csv, "adresse.csv")

    # insert one address to database
    def insert_addresses(self, address: dict, array_for_csv):
        matching_addresses: list[dict] = self._get_matching_addresses(address)
        if len(matching_addresses) == 1:
            combined_address_id = matching_addresses.__getitem__(0).get("ADRESS_ID")
            array_for_csv.append([combined_address_id, address.get("ADRESS_ID")])
            self.con_master.insert_address_datenherkunft(combined_address_id, 2)
        elif len(matching_addresses) == 0:
            try:
                # TODO manual nachfrage fuer land
                new_address_id: int = self.con_master.insert_address(land="Deutschland",
                                                                     plz=address.get("ORTSKENNZAHL"),
                                                                     ort=address.get("NAME"),
                                                                     strasse=address.get("STRASSE"),
                                                                     hausnummer=address.get("NUMMER"),
                                                                     bundesland=address.get("BEZEICHNUNG"))

                array_for_csv.append([new_address_id, address.get("ADRESS_ID")])
        else:
            # TODO manual auswahl zwischen eintraegen
            pass

    # tests if address exists in database combined
    def _get_matching_addresses(self, address_to_check: dict) -> list[dict]:
        return [address for address in self.f2_addresses_join
                if compare_strings(address.get("STRASSE"), address_to_check.get("STRASSE"))
                and compare_strings(address.get("ORTSKENNZAHL"), address_to_check.get("ORTSKENNZAHL"))
                and compare_strings(address.get("NUMMER"), address_to_check.get("NUMMER"))
                and compare_strings(address.get("NAME"), address_to_check.get("NAME"))]

    # search for existing address and get the id
    def get_id_of_existing_address(self, address_to_check: dict) -> int:
        comb_address_id: int = next(address.get("ADRESS_ID") for address in self.f2_addresses_join
                                    if compare_strings(address.get("STRASSE"), address_to_check.get("STRASSE"))
                                    and compare_strings(address.get("ORTSKENNZAHL"),
                                                        address_to_check.get("ORTSKENNZAHL"))
                                    and compare_strings(address.get("NUMMER"), address_to_check.get("NUMMER"))
                                    and compare_strings(address.get("NAME"), address_to_check.get("NAME")))
        # TODO exception wenn id nicht vorhanden
        return comb_address_id


if __name__ == "__main__":
    # create object
    addressMerge = AddressMerge()
    addressMerge.start()
