import unittest

from rudolf.etl_scripts.adresse import Adresse
from rudolf.tests.mock_sqlite_service import MockSQLiteService
from rudolf.tests.non_commit_oracle_service import NonCommitCombDBService, NonCommitF2DBService


# TODO Mock benutzen
class TestAddress(unittest.TestCase):

    def setUp(self) -> None:
        self.address_class = Adresse()
        self.address_class._con_f2 = NonCommitF2DBService()
        self.address_class._con_master = NonCommitCombDBService()

        self.test_f2_address: dict = {'STRASSE': 'Lauterberg', 'NUMMER': '143', 'ORTSKENNZAHL': '14727',
                                      'NAME': 'Premnitz', 'BEZEICHNUNG': 'Brandenburg', 'ADRESS_ID': 4611}
        self.matching_and_transferred_comb_address: list[dict] = [
            {'ADRESSE_ID': 12191, 'LAND': 'Deutschland', 'PLZ': '14727', 'ORT': 'Premnitz',
             'STRASSE': 'Lauterberg', 'HAUSNUMMER': '143', 'BUNDESLAND': 'Brandenburg'}]
        self.not_matching_transferred_comb_address: list[dict] = [
            {'ADRESSE_ID': 79, 'LAND': 'Deutschland', 'PLZ': '14727', 'ORT': 'Premnitz',
             'STRASSE': 'Am Lauterberg', 'HAUSNUMMER': '143', 'BUNDESLAND': 'Brandenburg'}]

    def test_get_matching_addresses_if_exists(self):
        # exists
        self.address_class._comb_addresses = self.matching_and_transferred_comb_address
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists(self.test_f2_address)) == 1)

        # not exists
        self.address_class._comb_addresses = self.not_matching_transferred_comb_address
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists(self.test_f2_address)) == 0)

        # empty comb list
        self.address_class._comb_addresses = []
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists(self.test_f2_address)) == 0)

        # empty f2_address
        self.address_class._comb_addresses = self.matching_and_transferred_comb_address
        self.assertTrue(len(self.address_class._get_matching_addresses_if_exists({})) == 0)

    def test_is_address_already_transferred(self):
        # exists
        self.assertTrue(self.address_class._is_address_already_transferred(6781))
        # not exists
        self.assertTrue(not self.address_class._is_address_already_transferred(0))
        # negative value
        self.assertTrue(not self.address_class._is_address_already_transferred(-58))

    def test_insert_addresses(self):
        self.address_class._con_rudolf = MockSQLiteService()
        # matching
        self.address_class._comb_addresses = self.matching_and_transferred_comb_address
        self.address_class._insert_addresses(self.test_f2_address)
        # new
        self.address_class._comb_addresses = self.not_matching_transferred_comb_address
        self.address_class._insert_addresses(self.test_f2_address)
