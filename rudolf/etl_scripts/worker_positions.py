from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.sqlite_service import SQLiteService


class WorkerPositions:

    def __init__(self):
        self.con_f2 = F2DBService()
        self.con_master = CombDBService()
        self.con_rudolf = SQLiteService()
        self._get_data_basis()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_all_positions: list[dict] = self.con_f2.select_all_funktionen()

    def start(self):
        for position in self.f2_all_positions:
            # TODO try/catch
            if not self._is_position_already_transferred(position.get("FUNKTIONS_ID")):
                self._insert_funktion(position)

    def _insert_funktion(self, position: dict) -> None:
        matching_position_entries: list[dict] = self._get_matching_positions_if_exists(position)
        if len(matching_position_entries) == 1:
            # TODO insert datenherkunft
            combined_position_id = matching_position_entries.__getitem__(0).get("FUNKTION_ID")
            self.con_rudolf.insert_id_allocation(config.WORKER_POSITION_TABLE, combined_position_id,
                                                 position.get("FUNKTIONS_ID"))
        elif len(matching_position_entries) == 0:
            new_position_id: int = self.con_master.insert_funktion(position.get("BEZEICHNUNG"))
            # TODO insert datenherkunft
            self.con_rudolf.insert_id_allocation(config.WORKER_POSITION_TABLE, new_position_id,
                                                 position.get("FUNKTIONS_ID"))
        else:
            pass
            # TODO manual check

    def _get_matching_positions_if_exists(self, position: dict) -> list[dict]:
        # TODO
        return []

    def _is_position_already_transferred(self, position_id: int) -> bool:
        return len(self.con_rudolf.select_where_old_id(table_name=config.WORKER_POSITION_TABLE,
                                                       old_id=position_id)) != 0
