import unittest

from rudolf.tests.non_commit_oracle_service import NonCommitCombDBService, NonCommitF2DBService


class TestF2DBService(unittest.TestCase):

    def setUp(self) -> None:
        self.oracle_service = NonCommitF2DBService()

    def test_select_all_addresses_join(self):
        self.oracle_service.select_all_addresses_join()

    def test_select_all_mitarbeiter_join_funktion(self):
        self.oracle_service.select_all_mitarbeiter_join_funktion()

    def test_select_all_lagerplaetze_join_produkt(self):
        self.oracle_service.select_all_lagerplaetze_join_produkt()


class TestCombDBService(unittest.TestCase):

    def setUp(self) -> None:
        self.oracle_service = NonCommitCombDBService()

    def test_select_produktkategorie_where_bezeichnung(self):
        subcategories: list[dict] = self.oracle_service.select_produktkategorie_where_bezeichnung(
            bezeichnung="Canned Fruit")
        self.assertIs(subcategories[0].get("PRODUKT_SUBKATEGORIE_ID"), 3)

    def test_select_addresses_where(self):
        addresses: list[dict] = self.oracle_service.select_addresses_where(strasse="Schwalbenweg",
                                                                           plz=57629, ort="Müschenbach",
                                                                           hausnummer="119 c")
        self.assertEqual(addresses[0].get("ADRESSE_ID"), 387)

    def test_insert_subcategory(self):
        new_id: int = self.oracle_service.insert_lieferant(adresse_id=12191,
                                                           email="TEST_MAIL",
                                                           lieferant_name="TEST LIEFERANT")
        max_id: int = self.oracle_service.select_max_id_from_table("LIEFERANT_ID", "LIEFERANT")
        self.assertTrue(new_id == max_id)

    def test_insert_address(self):
        new_id: int = self.oracle_service.insert_address(land="TESTLAND",
                                                         plz=0,
                                                         ort='TESTORT',
                                                         strasse='TESTSTRASSE',
                                                         hausnummer=0,
                                                         bundesland="TESTBUNDESLAND")
        max_id: int = self.oracle_service.select_max_id_from_table("ADRESSE_ID", "ADRESSE")
        self.assertTrue(new_id == max_id)

    def test_insert_lieferant(self):
        new_id: int = self.oracle_service.insert_subcategory(subcat_name="TEST SUBKATEGORIE")
        max_id: int = self.oracle_service.select_max_id_from_table("PRODUKT_SUBKATEGORIE_ID", "PRODUKT_SUBKATEGORIE")
        self.assertTrue(new_id == max_id)

    def test_insert_funktion(self):
        new_id: int = self.oracle_service.insert_funktion(bezeichnung="TESTFUNKTION")
        max_id: int = self.oracle_service.select_max_id_from_table("FUNKTION_ID", "FUNKTION")
        self.assertTrue(new_id == max_id)

    def test_insert_mitarbeiter(self):
        new_id: int = self.oracle_service.insert_mitarbeiter(
            anrede="TEST",
            vorname="TEST",
            nachname="TEST",
            email="TEST",
            gehalt=0,
            eintrittsdatum="3333-01-01",
            adresse_id=12191
        )
        max_id: int = self.oracle_service.select_max_id_from_table("MITARBEITER_ID", "MITARBEITER")
        self.assertTrue(new_id == max_id)

    def test_insert_mitarbeiter_provision(self):
        new_id: int = self.oracle_service.insert_mitarbeiter_provision(mitarbeiter_id=1, provisionssatz=0)
        max_id: int = self.oracle_service.select_max_id_from_table("PROVISION_ID", "PROVISION")
        self.assertTrue(new_id == max_id)

    def test_insert_mitarbeiter_funktion(self):
        new_id: int = self.oracle_service.insert_mitarbeiter_funktion(mitarbeiter_id=1, funktion_id=36)
        max_id: int = self.oracle_service.select_max_id_from_table("ZUWEISUNG_MITARBEITER_FUNKTION_ID",
                                                                   "ZUWEISUNG_MITARBEITER_FUNKTION")
        self.assertTrue(new_id == max_id)

    def test_insert_lagerplatz(self):
        new_id: int = self.oracle_service.insert_lagerplatz(lager_id=28, produkt_id=825, regal_reihe=14,
                                                            regal_spalte=28, akt_menge=14, regal_zeile=0)
        max_id: int = self.oracle_service.select_max_id_from_table("LAGERPLATZ_ID", "LAGERPLATZ")
        self.assertTrue(new_id == max_id)

    def test_insert_kunde(self):
        new_id: int = self.oracle_service.insert_kunde(anrede="TEST", vorname="TEST", nachname="TEST",
                                                       email="TEST", geburtsdatum="1970-01-02")
        max_id: int = self.oracle_service.select_max_id_from_table("KUNDE_ID", "KUNDE")
        self.assertTrue(new_id == max_id)

    def test_insert_kunde_adresse(self):
        new_id: int = self.oracle_service.insert_kunde_adresse(adress_id=2469, kunden_id=2529,
                                                               adressart="Lieferadresse")
        max_id: int = self.oracle_service.select_max_id_from_table("KUNDE_ADRESSE_ID", "KUNDE_ADRESSE")
        self.assertTrue(new_id == max_id)

    def test_insert_warenkorb(self):
        new_id: int = self.oracle_service.insert_warenkorb(kunden_id=2529, gesamtpreis=0)
        max_id: int = self.oracle_service.select_max_id_from_table("WARENKORB_ID", "WARENKORB")
        self.assertTrue(new_id == max_id)

    def test_insert_subcategory_datenherkunft(self):
        self.oracle_service.insert_subcategory_datenherkunft(subcat_id=111, datenherkunft_id=2)

    def test_insert_address_datenherkunft(self):
        self.oracle_service.insert_address_datenherkunft(address_id=2469, datenherkunft_id=2)

    def test_insert_lieferant_datenherkunft(self):
        self.oracle_service.insert_lieferant_datenherkunft(lieferant_id=118, datenherkunft_id=2)

    def test_insert_funktion_datenherkunft(self):
        self.oracle_service.insert_funktion_datenherkunft(funktion_id=36, datenherkunft_id=2)

    def test_insert_mitarbeiter_datenherkunft(self):
        self.oracle_service.insert_mitarbeiter_datenherkunft(mitarbeiter_id=1, datenherkunft_id=2)

    def test_insert_kunde_datenherkunft(self):
        self.oracle_service.insert_kunde_datenherkunft(kunden_id=2529, datenherkunft_id=2)
