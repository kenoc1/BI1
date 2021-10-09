from rudolf import config
from rudolf.etl_scripts.anreden_finder import AnredenFinder
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService
from rudolf.util import compare_strings


class Kunde:

    def __init__(self):
        self._init_db_connections()

    def init(self) -> None:
        self._get_data_basis()

    def _init_db_connections(self) -> None:
        # TODO try/catch
        self.con_f2 = F2DBService()
        self.con_master = CombDBService()
        self.con_rudolf = SQLiteService()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_customers: list[dict] = self.con_f2.select_all_kunden()
        self.comb_customers: list[dict] = self.con_master.select_all_kunden_join_adresse_where_rechnungsadresse()

    def start(self):
        for customer in self.f2_customers:
            # TODO try/catch
            if not self._is_customer_already_transferred(customer.get("KUNDEN_ID")):
                self._insert_customers(customer)

    def _insert_customers(self, f2_customer: dict):
        print(f2_customer)
        matching_customers: list[dict] = self._get_matching_customers_if_exists(f2_customer)
        if len(matching_customers) == 1:
            combined_kunden_id = matching_customers.__getitem__(0).get("KUNDE_ID")
            self.con_master.insert_kunde_datenherkunft(combined_kunden_id, config.SOURCE_F2)
            self.con_rudolf.insert_id_allocation(config.KUNDEN_TABLE, combined_kunden_id, f2_customer.get("KUNDEN_ID"))
        elif len(matching_customers) == 0:
            # {'KUNDE_ID': 3171, 'ANREDE': 'Herr', 'VORNAME': 'Garth', 'NACHNAME': 'Dixie', 'EMAIL': 'Keine E-Mail', 'GEBURTSDATUM': datetime.datetime(1992, 10, 7, 0, 0), 'KUNDE_ADRESSE_ID': 3972, 'ADRESSE_ID': 4478, 'ADRESSART': 'Lieferadresse'}
            new_kunden_id: int = self.con_master.insert_kunde(
                anrede=AnredenFinder().finde_anrede(f2_customer.get("VORNAME")),
                vorname=f2_customer.get("VORNAME"),
                nachname=f2_customer.get("NACHNAME"),
                email=config.DUMMY_MAIL,
                geburtsdatum=self._datetime_to_date_string(f2_customer.get("GEBURTSDATUM")))
            self.con_master.insert_kunde_datenherkunft(new_kunden_id, config.SOURCE_F2)
            self.con_rudolf.insert_id_allocation(config.KUNDEN_TABLE, new_kunden_id, f2_customer.get("KUNDEN_ID"))
            if f2_customer.get("RECHNUNGS_ADRESSE_ID"):
                comb_adresse_id_for_rechnung: int = \
                    self.con_rudolf.select_where_old_id(config.ADDRESS_TABLE,
                                                        f2_customer.get("RECHNUNGS_ADRESSE_ID"))
                self.con_master.insert_kunde_adresse(adress_id=comb_adresse_id_for_rechnung,
                                                     kunden_id=new_kunden_id,
                                                     adressart="Rechnungsadresse")
            if f2_customer.get("LIEFER_ADRESSE_ID"):
                comb_adresse_id_for_liefer: int = \
                    self.con_rudolf.select_where_old_id(config.ADDRESS_TABLE, f2_customer.get("LIEFER_ADRESSE_ID"))
                self.con_master.insert_kunde_adresse(adress_id=comb_adresse_id_for_liefer,
                                                     kunden_id=new_kunden_id,
                                                     adressart="Lieferadresse")
            self.con_master.insert_warenkorb(kunden_id=new_kunden_id, gesamtpreis=0)
        else:
            # TODO manual auswahl zwischen eintraegen
            pass

    def _get_matching_customers_if_exists(self, f2_customer: dict) -> list[dict]:
        return [comb_customer for comb_customer in self.comb_customers
                if compare_strings(comb_customer.get("VORNAME"), f2_customer.get("VORNAME"))
                and compare_strings(comb_customer.get("NACHNAME"), f2_customer.get("NACHNAME"))
                and comb_customer.get("ADRESSE_ID") ==
                self.con_rudolf.select_where_old_id(config.ADDRESS_TABLE, f2_customer.get("RECHNUNGS_ADRESSE_ID"))]

    def _is_customer_already_transferred(self, f2_kunden_id: int) -> bool:
        try:
            self.con_rudolf.select_where_old_id(table_name=config.KUNDEN_TABLE, old_id=f2_kunden_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False

    @staticmethod
    def _datetime_to_date_string(datetime) -> str:
        return str(datetime).split()[0]


if __name__ == "__main__":
    # create object
    customer_obj = Kunde()
    # addressMerge.start()
