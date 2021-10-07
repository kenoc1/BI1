import util
from db_service import DB_F2, DB_MASTER
from combined import key_allocation_reader, key_allocation_saver
import config


class ZwischenhaendlerMerger:

    def __init__(self):
        self.f2_con = DB_F2()
        self.combined_con = DB_MASTER()
        self.f2_lieferanten: list[dict] = self.f2_con.select_all_lieferant()
        self.f2_einkaeufe: list[dict] = self.f2_con.select_all_einkaeufe()
        self.f2_gewichtsbasiert_einkauf: list[dict] = self.f2_con.select_all_einkaeufe_gewichtsbasiert()
        self.f2_stueckbasiert_einkauf: list[dict] = self.f2_con.select_all_einkaeufe_stueckbasiert()
        self.product_id_allcoation = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(
            file_name=config.PRODUCTS_CON_FILE_NAME)
        self.address_id_allcoation = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(
            file_name=config.ADDRESS_CON_FILE_NAME)
        self.supplier_id_allcoation = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(
            file_name=config.SUPPLIER_CON_FILE_NAME)
        self.mitarbeiter_id_allcoation = key_allocation_reader.read_f2_to_comb_id_allocation_to_file(
            file_name=config.MITARBEITER_CON_FILE_NAME)

    def _insert_zwischenhaendler(self) -> None:
        for lieferant in self.f2_lieferanten:
            new_address_id: int = self._get_new_address_id(f2_address_id=lieferant.get("ADRESS_ID"))
            comb_zwischenhaendler_id: int = self.combined_con.insert_zwischenhaendler(name=lieferant.get("NAME"),
                                                                                      email=lieferant.get("EMAIL"),
                                                                                      nname=lieferant.get(
                                                                                          "NAME_ANSPRECHPARTNER"),
                                                                                      vname=lieferant.get(
                                                                                          "VORNAME_ANSPRECHPARTNER"),
                                                                                      adresseid=new_address_id)
            # TODO ID-CSV schreiben
            self.combined_con.insert_datenherkunft_zwischenhaendler(zwischenhaendler_id=comb_zwischenhaendler_id,
                                                                    datenherkunft_id=2)

    def _merge_einkauf(self) -> None:
        for einkauf in self.f2_einkaeufe:
            new_einkauf_id: int = self._insert_einkauf(einkauf)
            self._insert_einkauf_produkt(einkauf=einkauf, comb_einkauf_id=new_einkauf_id)

    @staticmethod
    def _calculate_menge_from_gewicht(menge: float) -> float:
        return menge / 2

    def _insert_einkauf(self, einkauf: dict) -> int:
        comb_lieferant_id: int = self._get_new_lieferant_id(f2_lieferant_id=einkauf.get("LIEFERANT_ID"))
        comb_mitarbeiter_id: int = self._get_new_mitarbeiter_id(f2_mitarbeiter_id=einkauf.get("MITARBEITER_ID"))
        # TODO ID-CSV schreiben
        return self.combined_con.insert_einkauf(einkauf.get("EINKAUFSDATUM"), comb_lieferant_id, comb_mitarbeiter_id, 2)

    def _insert_einkauf_produkt(self, einkauf: dict, comb_einkauf_id: int) -> None:
        gewichtsbasiert: list[dict] = [gb_ek for gb_ek in self.f2_gewichtsbasiert_einkauf if
                                       gb_ek.get("EINKAUFS_ID") == einkauf.get("EINKAUFS_ID")]
        [eintrag.update({"ANZAHL_PRODUKTE": self._calculate_menge_from_gewicht(eintrag.get("GEWICHT"))}) for eintrag
         in gewichtsbasiert]
        stueckbasiert: list[dict] = [sb_ek for sb_ek in self.f2_stueckbasiert_einkauf if
                                     sb_ek.get("EINKAUFS_ID") == einkauf.get("EINKAUFS_ID")]
        for produkt_im_einkauf in gewichtsbasiert + stueckbasiert:
            self.combined_con.insert_einkauf_produkt(einkaufid=comb_einkauf_id,
                                                     produktid=produkt_im_einkauf.get("PRODUKT_ID"),
                                                     menge=produkt_im_einkauf.get("ANZAHL_PRODUKTE"))

    def _get_new_product_id(self, f2_product_id: int) -> int:
        return util.search_for_id(self.product_id_allcoation, f2_product_id)

    def _get_new_lieferant_id(self, f2_lieferant_id: int) -> int:
        return util.search_for_id(self.supplier_id_allcoation, f2_lieferant_id)

    def _get_new_mitarbeiter_id(self, f2_mitarbeiter_id: int) -> int:
        return util.search_for_id(self.mitarbeiter_id_allcoation, f2_mitarbeiter_id)

    def _get_new_address_id(self, f2_address_id: int) -> int:
        return util.search_for_id(self.address_id_allcoation, f2_address_id)
