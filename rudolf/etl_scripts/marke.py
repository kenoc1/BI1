from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService


class Marke:
    def __init__(self):
        self._init_db_connections()

    def init(self) -> None:
        self._get_data_basis()

    def _init_db_connections(self) -> None:
        # TODO try/catch
        self._db_f2 = F2DBService()
        self._db_master = CombDBService()
        self._db_rudolf = SQLiteService()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_marken: list[dict] = self._db_f2.select_all_marken()

    def start(self):
        for marke in self.f2_marken:
            if not self._is_marke_already_transferred(marke.get("MARKE_ID")):
                self._insert_marke(marke)

    def _insert_marke(self, marke: dict) -> None:
        print(marke)
        supplier_id = self._get_new_supplier_id(marke["HERSTELLER_ID"])
        brand_name = marke["BEZEICHNUNG"]
        new_marke_id = self._db_master.insert_marke(supplier_id=supplier_id, brand_name=brand_name)
        self._db_rudolf.insert_id_allocation(config.MARKE_TABLE, new_marke_id, marke["MARKE_ID"])

    def _get_new_supplier_id(self, f2_supplier_id: int) -> int:
        return self._db_rudolf.select_where_old_id(config.LIEFERANT_HERSTELLER_TABLE, f2_supplier_id)

    def _is_marke_already_transferred(self, f2_marke_id: int) -> bool:
        try:
            self._db_rudolf.select_where_old_id(table_name=config.MARKE_TABLE, old_id=f2_marke_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False
