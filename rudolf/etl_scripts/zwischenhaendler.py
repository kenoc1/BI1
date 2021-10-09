from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService


class Zwischenhaendler:

    def __init__(self):
        self._init_db_connections()

    def init(self) -> None:
        self._get_data_basis()

    def _init_db_connections(self) -> None:
        # TODO try/catch
        self.f2_con = F2DBService()
        self.combined_con = CombDBService()
        self.rudolf_con = SQLiteService()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_lieferanten: list[dict] = self.f2_con.select_all_lieferant()

    def start(self):
        for lieferant in self.f2_lieferanten:
            # TODO try/catch
            if not self._is_zwhaendler_already_transferred(lieferant.get("LIEFERANT_ID")):
                self.merge_zwischenhaendler(lieferant)

    def merge_zwischenhaendler(self, lieferant: dict) -> None:
        # TODO was, wenn zwhaendler auf datendatz abbildbar
        new_address_id: int = self._get_new_address_id(f2_address_id=lieferant.get("ADRESS_ID"))
        comb_zwischenhaendler_id: int = self.combined_con.insert_zwischenhaendler(name=lieferant.get("NAME"),
                                                                                  email=lieferant.get("EMAIL"),
                                                                                  nname=lieferant.get(
                                                                                      "NAME_ANSPRECHPARTNER"),
                                                                                  vname=lieferant.get(
                                                                                      "VORNAME_ANSPRECHPARTNER"),
                                                                                  adresseid=new_address_id)
        self.combined_con.insert_datenherkunft_zwischenhaendler(zwischenhaendler_id=comb_zwischenhaendler_id,
                                                                datenherkunft_id=2)
        self.rudolf_con.insert_id_allocation(config.ZWHAENDLER_TABLE, comb_zwischenhaendler_id,
                                             lieferant.get("LIEFERANT_ID"))

    def _get_new_address_id(self, f2_address_id: int) -> int:
        return self.rudolf_con.select_where_old_id(table_name=config.ADDRESS_TABLE, old_id=f2_address_id)

    def _is_zwhaendler_already_transferred(self, f2_zwhaendler_id: int) -> bool:
        try:
            self.rudolf_con.select_where_old_id(table_name=config.ZWHAENDLER_TABLE, old_id=f2_zwhaendler_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False


if __name__ == "__main__":
    zwischenhaendler_merger = Zwischenhaendler()
    # zwischenhaendler_merger.merge_zwischenhaendler()
    # zwischenhaendler_merger.merge_einkauf()
