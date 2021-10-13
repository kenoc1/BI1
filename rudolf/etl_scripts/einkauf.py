import datetime

from rudolf import config, util
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService


class Einkauf:
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
        self.f2_einkaeufe: list[dict] = self.f2_con.select_all_einkaeufe()
        self.f2_gewichtsbasiert_einkauf: list[dict] = self.f2_con.select_all_einkaeufe_gewichtsbasiert()
        self.f2_stueckbasiert_einkauf: list[dict] = self.f2_con.select_all_einkaeufe_stueckbasiert()

    def start(self):
        for einkauf in self.f2_einkaeufe:
            if not self._is_einkauf_already_transferred(einkauf.get("EINKAUFS_ID")):
                self.insert_einkauf(einkauf)

    def insert_einkauf(self, einkauf: dict) -> None:
        if not self._is_einkauf_empty(einkauf):
            print(einkauf)
            new_einkauf_id: int = self._insert_einkauf(einkauf)
            self._write_id_allocation_to_rudolf_db(new_einkauf_id, einkauf.get("EINKAUFS_ID"))
            self._insert_einkauf_produkt(f2_einkauf=einkauf, comb_einkauf_id=new_einkauf_id)
        else:
            self._write_id_allocation_to_rudolf_db(None, einkauf.get("EINKAUFS_ID"))

    def _is_einkauf_empty(self, einkauf: dict) -> bool:
        return len([gb_ek for gb_ek in self.f2_gewichtsbasiert_einkauf if
                    gb_ek.get("EINKAUFS_ID") == einkauf.get("EINKAUFS_ID")] +
                   [sb_ek for sb_ek in
                    self.f2_stueckbasiert_einkauf if
                    sb_ek.get("EINKAUFS_ID") == einkauf.get("EINKAUFS_ID")]) < 1

    @staticmethod
    def _calculate_menge_from_gewicht(menge: float) -> float:
        return menge / 2

    def _insert_einkauf(self, einkauf: dict) -> int:
        comb_lieferant_id: int = self._get_new_lieferant_id(f2_lieferant_id=einkauf.get("LIEFERANT_ID"))
        comb_mitarbeiter_id: int = self._get_new_mitarbeiter_id(f2_mitarbeiter_id=einkauf.get("MITARBEITER_ID"))
        print(einkauf)
        return self.combined_con.insert_einkauf(util.datetime_to_date_string(einkauf.get("EINKAUFSDATUM")), comb_lieferant_id, comb_mitarbeiter_id, 2)

    def _insert_einkauf_produkt(self, f2_einkauf: dict, comb_einkauf_id: int) -> None:
        gewichtsbasiert: list[dict] = [gb_ek for gb_ek in self.f2_gewichtsbasiert_einkauf if
                                       gb_ek.get("EINKAUFS_ID") == f2_einkauf.get("EINKAUFS_ID")]
        [eintrag.update({"ANZAHL_PRODUKTE": self._calculate_menge_from_gewicht(eintrag.get("GEWICHT"))}) for eintrag
         in gewichtsbasiert]
        stueckbasiert: list[dict] = [sb_ek for sb_ek in self.f2_stueckbasiert_einkauf if
                                     sb_ek.get("EINKAUFS_ID") == f2_einkauf.get("EINKAUFS_ID")]
        for produkt_im_einkauf in gewichtsbasiert + stueckbasiert:
            comb_product_id: int = self._get_new_product_id(produkt_im_einkauf.get("PRODUKT_ID"))
            self.combined_con.insert_einkauf_produkt(einkaufid=comb_einkauf_id,
                                                     produktid=comb_product_id,
                                                     menge=produkt_im_einkauf.get("ANZAHL_PRODUKTE"))

    def _get_new_product_id(self, f2_product_id: int) -> int:
        return self.rudolf_con.select_where_old_id(table_name=config.PRODUKTE_TABLE, old_id=f2_product_id)

    def _get_new_lieferant_id(self, f2_lieferant_id: int) -> int:
        return self.rudolf_con.select_where_old_id(table_name=config.ZWHAENDLER_TABLE, old_id=f2_lieferant_id)

    def _get_new_mitarbeiter_id(self, f2_mitarbeiter_id: int) -> int:
        return self.rudolf_con.select_where_old_id(table_name=config.WORKER_TABLE, old_id=f2_mitarbeiter_id)

    def _write_id_allocation_to_rudolf_db(self, comb_id: int, f2_id: int):
        self.rudolf_con.insert_id_allocation(config.EINKAUF_TABLE, comb_id, f2_id)

    def _is_einkauf_already_transferred(self, f2_einkauf_id: int) -> bool:
        try:
            self.rudolf_con.select_where_old_id(table_name=config.EINKAUF_TABLE, old_id=f2_einkauf_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False
