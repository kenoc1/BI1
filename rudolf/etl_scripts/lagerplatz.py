from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService


class Lagerplatz:
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
        self.f2_lagerplaetze: list[dict] = self.con_f2.select_all_lagerplaetze_join_produkt()

    def start(self):
        for lagerplatz in self.f2_lagerplaetze:
            # TODO try/catch
            if not self._is_lagerplatz_already_transferred(lagerplatz.get("LAGERPLATZ_ID")):
                self._insert_lagerplaetze(lagerplatz)

    def _insert_lagerplaetze(self, lager: dict):
        #  {'LAGERPLATZ_ID': 2553, 'REGAL_NUMMER': 103, 'REGALZEILE': 0, 'REGALSPALTE': 1, 'REGALBREITE': 12.0, 'REGALTIEFE': 20.0, 'REGALHÖHE': 16.0, 'TYP': 'Lagerflaeche', 'PRODUKT_ID': 2905}
        new_lagerplatz_id: int = self.con_master.insert_lagerplatz(
            lager_id=config.F2_VERKAUFSFLAECHE_LAGER_ID if lager.get(
                "TYP") == "Verkaufsflaeche" else config.F2_LAGERFLAECHE_LAGER_ID,
            produkt_id=1,
            regal_reihe=lager.get("REGALZEILE"),
            regal_spalte=lager.get("REGAL_NUMMER"),
            regal_zeile=lager.get("REGALSPALTE"),
            # TODO ist das richtig so? wurde das nicht inzwischen anders besprochen?
            akt_menge=-100000)
        self.con_rudolf.insert_id_allocation(config.ADDRESS_TABLE, new_lagerplatz_id, lager.get("LAGERPLATZ_ID"))

    def _is_lagerplatz_already_transferred(self, f2_lagerplatz_id: int) -> bool:
        try:
            self.con_rudolf.select_where_old_id(table_name=config.LAGERPLATZ_TABLE, old_id=f2_lagerplatz_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False


if __name__ == "__main__":
    # create object
    Lagerplatz()  # .start()
