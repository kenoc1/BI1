from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService


class Preishistorie:
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
        self.f2_preishistorie: list[dict] = self._db_f2.select_all_preise()

    def start(self):
        for preis in self.f2_preishistorie:
            if not self._is_preis_already_transferred(preis.get("PREIS_ID")):
                self._insert_preis(preis)

    def _insert_preis(self, preis: dict) -> None:
        f2_product_id = preis["PRODUKT_ID"]
        product_id = self._get_new_product_id(f2_product_id)
        price = preis["BETRAG"]
        if preis["TYP"] == config.PREIS_TYP_F2[0]:
            typ = config.PREIS_TYP[0]
        else:
            typ = config.PREIS_TYP[1]
        start_date = preis["GUELTIGKEITS_BEGINN"]
        self._db_master.insert_product_price_history(product_id=product_id, price=price, typ=typ,
                                                     start_date=start_date)
        self._db_rudolf.insert_id_allocation(config.PREIS_TABLE, None, preis.get("PREIS_ID"))

    def _is_preis_already_transferred(self, f2_preis_id: int) -> bool:
        try:
            self._db_rudolf.select_where_old_id(table_name=config.PREIS_TABLE, old_id=f2_preis_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False

    def _get_new_product_id(self, f2_product_id: int) -> int:
        return self._db_rudolf.select_where_old_id(config.PRODUKTE_TABLE, f2_product_id)
