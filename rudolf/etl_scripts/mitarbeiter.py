from rudolf import config
from rudolf.etl_scripts.anreden_finder import AnredenFinder
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService
from rudolf.util import compare_strings


class Mitarbeiter:

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
        self.f2_all_mitarbeiter: list[dict] = self.con_f2.select_all_mitarbeiter_join_funktion()
        self.comb_all_mitarbeiter: list[dict] = self.con_master.select_all_mitarbeiter()

    def start(self):
        for mitarbeiter in self.f2_all_mitarbeiter:
            # TODO try/catch
            if not self._is_mitarbeiter_already_transferred(mitarbeiter.get("MITARBEITER_ID")):
                self._insert_mitarbeiter(mitarbeiter)

    def _insert_mitarbeiter(self, mitarbeiter: dict) -> None:
        # {'MITARBEITER_ID': 258, 'VORNAME': 'John', 'NACHNAME': 'Lamb', 'ADRESS_ID': 4615, 'GEHALT': 5380.57,
        # 'PROVISIONSSATZ': 0.37, 'FUNKTIONS_ID': 35, 'BEZEICHNUNG': 'Einkäufer'}
        matching_mitarbeiter_entries: list[dict] = self._get_matching_workers_if_exists(mitarbeiter)
        if len(matching_mitarbeiter_entries) == 1:
            combined_mitarbeiter_id = matching_mitarbeiter_entries.__getitem__(0).get("MITARBEITER_ID")
            self.con_master.insert_mitarbeiter_provision(combined_mitarbeiter_id, mitarbeiter.get("PROVISIONSSATZ"))
            self.con_master.insert_mitarbeiter_datenherkunft(combined_mitarbeiter_id, config.SOURCE_F2)
            self.con_master.insert_mitarbeiter_funktion(combined_mitarbeiter_id, mitarbeiter.get("FUNKTIONS_ID"))
            self.con_rudolf.insert_id_allocation(config.WORKER_TABLE, combined_mitarbeiter_id,
                                                 mitarbeiter.get("MITARBEITER_ID"))
        elif len(matching_mitarbeiter_entries) == 0:
            new_mitarbeiter_id: int = self.con_master.insert_mitarbeiter(
                anrede=AnredenFinder().finde_anrede(mitarbeiter.get("VORNAME")),
                vorname=mitarbeiter.get("VORNAME"),
                nachname=mitarbeiter.get("NACHNAME"),
                email=config.DUMMY_MAIL,
                gehalt=mitarbeiter.get("GEHALT"),
                eintrittsdatum="3333-01-01",
                adresse_id=self.con_rudolf.select_where_old_id(
                    table_name=config.ADDRESS_TABLE,
                    old_id=int(mitarbeiter.get("ADRESS_ID")))
                )
            self.con_master.insert_mitarbeiter_provision(new_mitarbeiter_id, mitarbeiter.get("PROVISIONSSATZ"))
            self.con_master.insert_mitarbeiter_datenherkunft(new_mitarbeiter_id, config.SOURCE_F2)
            self.con_master.insert_mitarbeiter_funktion(new_mitarbeiter_id, mitarbeiter.get("FUNKTIONS_ID"))
            self.con_rudolf.insert_id_allocation(config.WORKER_TABLE, new_mitarbeiter_id,
                                                 mitarbeiter.get("MITARBEITER_ID"))
        else:
            pass
            # TODO manual check

    def _get_matching_workers_if_exists(self, f2_mitarbeiter: dict) -> list[dict]:
        return [comb_worker for comb_worker in self.comb_all_mitarbeiter
                if compare_strings(comb_worker.get("VORNAME"), f2_mitarbeiter.get("VORNAME"))
                and compare_strings(comb_worker.get("NACHNAME"), f2_mitarbeiter.get("NACHNAME"))
                and comb_worker.get("ADRESSE_ID") ==
                self.con_rudolf.select_where_old_id(config.WORKER_TABLE, f2_mitarbeiter.get("ADRESS_ID"))]

    def _is_mitarbeiter_already_transferred(self, old_worker_id: int) -> bool:
        try:
            self.con_rudolf.select_where_old_id(table_name=config.WORKER_TABLE,
                                                old_id=old_worker_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False


if __name__ == "__main__":
    pass
