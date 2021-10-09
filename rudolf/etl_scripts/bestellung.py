import sys

import cx_Oracle

from rudolf import config
from rudolf.oracle_service import CombDBService, F2DBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService


class Bestellung:

    def __init__(self):
        self._init_db_connections()

    def init(self):
        self._get_data_basis()

    def _init_db_connections(self):
        # TODO try/catch
        self.f2_con = F2DBService()
        self.combined_con = CombDBService()
        self._con_rudolf = SQLiteService()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_sales: list[dict] = self.f2_con.select_all_sales()
        self.f2_rechnungsdaten: list[dict] = self.f2_con.select_all_rechnung()
        self.f2_bondaten: list[dict] = self.f2_con.select_all_bons()
        self.f2_lieferschein: list[dict] = self.f2_con.select_all_lieferschein()
        self.comb_warenkoerbe: list[dict] = self.combined_con.select_all_warenkoerbe()
        self.f2_gewichtsbasiert_im_verkauf = self.f2_con.select_all_gewichtsbasiert_verkauf()
        self.f2_stueckzahlbasiert_im_verkauf = self.f2_con.select_all_stueckzahlbasiert_verkauf()

    def start(self) -> None:
        try:
            for sale in self.f2_sales:
                if not self._is_bestellung_already_transferred(sale.get("VERKAUFS_ID")):
                    # TODO funktioniert nicht
                    new_bestellung_id: int = self._create_bestellung(f2_verkauf_entry=sale)
                    self._create_verkaufsdokumente(new_bestellung_id=new_bestellung_id)
                    self._create_bestellposition(new_bestellung_id=new_bestellung_id, verkauf=sale)
                    print("Created Bestellung: {}".format(str(sale)))
        except cx_Oracle.Error as error:
            print('Database error occurred:')
            print(error)
            sys.exit("Datenbankverbindung erzeugt Fehler, Skript wird gestoppt!")

    def _create_bestellung(self, f2_verkauf_entry: dict) -> int:
        print(f2_verkauf_entry)
        verkaufsdatum = f2_verkauf_entry.get("VERKAUFDATUM")
        f2_kunden_id: int = f2_verkauf_entry.get("KUNDEN_ID")
        com_kunden_id: int = self._con_rudolf.select_where_old_id(config.KUNDEN_TABLE,
                                                                  f2_kunden_id) if f2_kunden_id else config.DUMMY_KUNDE
        f2_mitarbeiter_id: int = f2_verkauf_entry.get("MITARBEITER_ID")
        com_mitarbeiter_id: int = self._con_rudolf.select_where_old_id(config.WORKER_TABLE, f2_mitarbeiter_id)
        warenkorb_id: int = next((elem.get("WARENKORB_ID") for elem in self.comb_warenkoerbe if
                                  elem.get("KUNDE_ID") == com_kunden_id), None)
        new_bestellung_id: int = self.combined_con.insert_bestellung(warenkorb_id=warenkorb_id,
                                                                     status="Abgeschlossen",
                                                                     bestelldatum=verkaufsdatum,
                                                                     datenherkunft=2,
                                                                     mitarbeiter_id=com_mitarbeiter_id)
        self._con_rudolf.insert_id_allocation(config.BESTELLUNG_TABLE, new_bestellung_id,
                                              f2_verkauf_entry.get("VERKAUFS_ID"))
        return new_bestellung_id

    def _create_verkaufsdokumente(self, new_bestellung_id: int) -> None:
        bon_element: dict = next((elem for elem in self.f2_bondaten if
                                  elem.get("VERKAUFS_ID") == new_bestellung_id), None)
        if bon_element:
            self._create_bon(bon_element, new_bestellung_id)
            self.combined_con.insert_bestellung_to_zahlungsart(bestellungid=new_bestellung_id,
                                                               zahlungsart_id=41 if bon_element.get(
                                                                   "ZAHLUNGSART") == "Bar" else 3)
        rechnung_element: dict = next((elem for elem in self.f2_rechnungsdaten if
                                       elem.get("VERKAUFS_ID") == new_bestellung_id), None)

        if rechnung_element:
            self._create_rechnung(rechnung_element, new_bestellung_id)
            self.combined_con.insert_bestellung_to_zahlungsart(bestellungid=new_bestellung_id,
                                                               zahlungsart_id=2)

        lieferschein_element: dict = next((elem for elem in self.f2_lieferschein if
                                           elem.get("VERKAUFS_ID") == new_bestellung_id), None)
        if lieferschein_element:
            self._create_lieferschein(lieferschein_element, new_bestellung_id)

    def _create_bon(self, bon: dict, bestellung_id: int) -> int:
        return self.combined_con.insert_bon(bestellungid=bestellung_id, gegebenesgeld=bon.get("GEGEBENES_GELD"),
                                            rueckgeld=bon.get("RUECKGELD"), summe_netto=0, summe_brutto=0)

    def _create_rechnung(self, rechnung: dict, bestellung_id: int) -> int:
        return self.combined_con.insert_rechnung(bestellungid=bestellung_id,
                                                 rechnungsdatum=rechnung.get("ABGLEICH_DATUM"),
                                                 summe_netto=0, summe_brutto=0)

    def _create_lieferschein(self, lieferschein: dict, bestellung_id: int):
        return self.combined_con.insert_lieferschein(bestellungid=bestellung_id,
                                                     lieferdatum=lieferschein.get("LIEFERDATUM"),
                                                     lieferkosten=lieferschein.get("LIEFER_KOSTEN"))

    def _create_bestellposition(self, verkauf: dict, new_bestellung_id: int):
        gewichtsbasierte_produkte: list[dict] = [elem for elem in self.f2_gewichtsbasiert_im_verkauf if
                                                 elem.get("VERKAUFS_ID") == verkauf.get("VERKAUFS_ID")]
        [eintrag.update({"ANZAHL_PRODUKTE": self._calculate_menge_from_gewicht(eintrag.get("GEWICHT"))}) for eintrag in
         gewichtsbasierte_produkte]
        stueckbasierte_produkte: list[dict] = [elem for elem in self.f2_stueckzahlbasiert_im_verkauf if
                                               elem.get("VERKAUFS_ID") == verkauf.get("VERKAUFS_ID")]
        for produkt_im_verkauf in gewichtsbasierte_produkte + stueckbasierte_produkte:
            self.combined_con.insert_bestellposition(bestellungid=new_bestellung_id,
                                                     produktid=self._con_rudolf.select_where_old_id(
                                                         config.PRODUKTE_TABLE, produkt_im_verkauf.get("PRODUKT_ID")),
                                                     menge=produkt_im_verkauf.get("ANZAHL_PRODUKTE"))

    @staticmethod
    def _calculate_menge_from_gewicht(menge: float) -> float:
        return menge / 2

    def _is_bestellung_already_transferred(self, f2_verkauf_id: int) -> bool:
        try:
            self._con_rudolf.select_where_old_id(table_name=config.BESTELLUNG_TABLE, old_id=f2_verkauf_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False


if __name__ == "__main__":
    b = Bestellung()
    b.init()
    b.start()
