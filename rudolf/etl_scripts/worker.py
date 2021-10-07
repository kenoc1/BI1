from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.sqlite_service import SQLiteService


class Worker:

    def __init__(self):
        self.con_f2 = F2DBService()
        self.con_master = CombDBService()
        self.con_rudolf = SQLiteService()
        self._get_data_basis()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_all_mitarbeiter: list[dict] = self.con_f2.select_all_mitarbeiter()

    def start(self):
        for mitarbeiter in self.f2_all_mitarbeiter:
            # TODO try/catch
            if not self._is_mitarbeiter_already_transferred(mitarbeiter.get("MITARBEITER_ID")):
                self._insert_mitarbeiter(mitarbeiter)

    def _insert_mitarbeiter(self, mitarbeiter: dict) -> None:
        matching_mitarbeiter_entries: list[dict] = self._get_matching_workers_if_exists(mitarbeiter)
        if len(matching_mitarbeiter_entries) == 1:
            combined_position_id = matching_mitarbeiter_entries.__getitem__(0).get("MITARBEITER_ID")
            # TODO insert provision
            # TODO insert datenherkunft
            # TODO insert zuweisung_mitarbeiter_funktion
            self.con_rudolf.insert_id_allocation(config.WORKER_TABLE, combined_position_id,
                                                 mitarbeiter.get("MITARBEITER_ID"))
        elif len(matching_mitarbeiter_entries) == 0:
            new_position_id: int = 0
            # TODO insert mitarbeiter
            # TODO insert provision
            # TODO insert datenherkunft
            # TODO insert zuweisung_mitarbeiter_funktion
            self.con_rudolf.insert_id_allocation(config.WORKER_TABLE, new_position_id,
                                                 mitarbeiter.get("MITARBEITER_ID"))
        else:
            pass
            # TODO manual check

    def _get_matching_workers_if_exists(self, mitarbeiter: dict) -> list[dict]:
        # TODO
        return []

    def _is_mitarbeiter_already_transferred(self, worker_id: int) -> bool:
        return len(self.con_rudolf.select_where_old_id(table_name=config.WORKER_TABLE,
                                                       old_id=worker_id)) != 0


if __name__ == "__main__":
    pass
