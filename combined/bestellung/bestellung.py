import sys
from typing import Optional

import cx_Oracle

from combined import file_writer
from db_service import DB_F2, DB_MASTER


class Bestellung:

    def __init__(self):
        self.f2_con = DB_F2()
        self.combined_con = DB_MASTER()
        self.f2_rechnungsdaten: list[dict] = self.f2_con.select_all_rechnung()
        self.f2_bondaten: list[dict] = self.f2_con.select_all_bons()
        self.f2_lieferschein: list[dict] = self.f2_con.select_all_lieferschein()

    def _verkauf_to_bestellung(self):
        f2_sales: list[dict] = self.f2_con.select_all_sales()
        for sale in f2_sales:
            verkaufsdatum = sale.get("VERKAUFDATUM")
            f2_kunden_id: int = sale.get("KUNDEN_ID")
            com_kunden_id: int = self._get_com_kundenid_by_f2_kundenid(
                f2_kunden_id) if f2_kunden_id else self.get_dummy_kunde_id()
            f2_mitarbeiter_id: int = sale.get("MITARBEITER_ID")
            com_mitarbeiter_id: int = self._get_com_mitarbeiterid_by_f2_mitarbeiterid(f2_mitarbeiter_id)
            warenkorb_id: int = self._get_warenkorb(com_kunden_id).__getitem__(0).get("WARENKORB_ID")
            new_bestellung_id: int = self.combined_con.insert_bestellung(warenkorb_id=warenkorb_id,
                                                                         status="Abgeschlossen",
                                                                         bestelldatum=verkaufsdatum,
                                                                         datenherkunft=2,
                                                                         mitarbeiter_id=com_mitarbeiter_id)
            # Bruttosumme und Nettosumme in Bon/Rechnung
            bon_element: dict = [elem.get("VERKAUFS_ID") for elem in self.f2_bondaten if
                                 elem.get("VERKAUFS_ID") == new_bestellung_id].__getitem__(0)
            if bon_element:
                comb_bon_id: int = self._create_bon(bon_element, new_bestellung_id)
                file_writer.write_to_csv([], [comb_bon_id, bon_element.get("BON_NUMMER")], "bon_ids_new_to_old.csv")

            rechnung_element: dict = [elem.get("VERKAUFS_ID") for elem in self.f2_rechnungsdaten if
                                      elem.get("VERKAUFS_ID") == new_bestellung_id].__getitem__(0)

            if rechnung_element:
                comb_rechnung_id: int = self._create_rechnung(rechnung_element, new_bestellung_id)
                file_writer.write_to_csv([], [comb_rechnung_id, rechnung_element.get("RECHNUNG_NUMMER")],
                                         "rechnung_ids_new_to_old.csv")

            lieferschein_element: dict = [elem.get("VERKAUFS_ID") for elem in self.f2_lieferschein if
                                          elem.get("VERKAUFS_ID") == new_bestellung_id].__getitem__(0)
            if lieferschein_element:
                comb_lieferschein_id: int = self._create_lieferschein(lieferschein_element, new_bestellung_id)
                file_writer.write_to_csv([], [comb_lieferschein_id, lieferschein_element.get("LIEFERSCHEIN_NUMMER")],
                                         "lieferschein_ids_new_to_old.csv")

    def _get_warenkorb(self, kunden_id: int) -> list[dict]:
        try:
            return self.combined_con.select_warenkorb_by_kundenid(kunden_id=kunden_id)
        except cx_Oracle.Error as error:
            print('Database error occurred:')
            print(error)
            sys.exit("Datenbankverbindung erzeugt Fehler, Skript wird gestoppt!")

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

    def _write_ids_to_file(self, bestellung_id: Optional[int], bon_id: Optional[int], rechnung_id: Optional[int],
                           liefer_id: Optional[int]):
        file_writer.write_to_csv([], [])

    def _get_com_kundenid_by_f2_kundenid(self, f2_kunden_id: int):
        # TODO csv einlesen und auswerten
        return 0

    def _get_com_mitarbeiterid_by_f2_mitarbeiterid(self, f2_kunden_id: int):
        # TODO csv einlesen und auswerten
        return 0

    def get_dummy_kunde_id(self):
        # TODO statische Filial2 Dummy-kunden id herausfinden
        return 0

# TODO Zahlungsart
# TODO Bestellung
