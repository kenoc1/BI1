import unittest

from rudolf.etl_scripts.address import Address


class TestAddress(unittest.TestCase):

    def setUp(self) -> None:
        self.address_class = Address()
        self.address_class._init_db_connections()
        self.test_f2_address: dict = {'STRASSE': 'Lauterberg', 'NUMMER': '143', 'ORTSKENNZAHL': '14727',
                                      'NAME': 'Premnitz', 'BEZEICHNUNG': 'Brandenburg', 'ADRESS_ID': 4611}
        self.matching_comb_address: list[dict] = [
            {'ADRESSE_ID': 12191, 'LAND': 'Deutschland', 'PLZ': '14727', 'ORT': 'Premnitz',
             'STRASSE': 'Lauterberg', 'HAUSNUMMER': '143', 'BUNDESLAND': 'Brandenburg'}]
        self.not_matching_comb_address: list[dict] = [
            {'ADRESSE_ID': 79, 'LAND': 'Deutschland', 'PLZ': '14727', 'ORT': 'Premnitz',
             'STRASSE': 'Am Lauterberg', 'HAUSNUMMER': '143', 'BUNDESLAND': 'Brandenburg'}]

    def test_get_matching_addresses_if_exists(self):
        # exists
        self.address_class._comb_addresses = self.matching_comb_address
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists(self.test_f2_address)) == 1)

        # not exists
        self.address_class._comb_addresses = self.not_matching_comb_address
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists(self.test_f2_address)) == 0)

        # empty comb list
        self.address_class._comb_addresses = []
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists(self.test_f2_address)) == 0)

        # empty f2_address
        self.address_class._comb_addresses = self.matching_comb_address
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists({})) == 0)

    def test_is_address_already_transferred(self):
        # exists
        self.assertTrue(self.address_class._is_address_already_transferred(6781))
        # not exists
        self.assertTrue(not self.address_class._is_address_already_transferred(0))
        # negative value
        self.assertTrue(not self.address_class._is_address_already_transferred(-58))

    def test_insert_address(self):
        new_address_id: int = self.address_class._con_master.insert_address(land="TESTLAND",
                                                                            plz=0,
                                                                            ort='TESTORT',
                                                                            strasse='TESTSTRASSE',
                                                                            hausnummer=0,
                                                                            bundesland="TESTBUNDESLAND")

