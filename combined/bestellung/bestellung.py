import sys

import cx_Oracle

from db_service import DB_F2, DB_MASTER


class Bestellung:

    def __init__(self):
        self.f2_con = DB_F2()
        self.combined_con = DB_MASTER()

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
            self.combined_con.insert_bestellung(warenkorb_id=warenkorb_id,
                                                status="Abgeschlossen",
                                                bestelldatum=verkaufsdatum,
                                                datenherkunft=2,
                                                mitarbeiter_id=com_mitarbeiter_id)
            # Bruttosumme und Nettosumme in Bon/Rechnung
            if True:
                self._create_bon()
            if True:
                self._create_rechnung()
            if True:
                self._create_lieferschein()

    def _get_warenkorb(self, kunden_id: int) -> list[dict]:
        try:
            return self.combined_con.select_warenkorb_by_kundenid(kunden_id=kunden_id)
        except cx_Oracle.Error as error:
            print('Database error occurred:')
            print(error)
            sys.exit("Datenbankverbindung erzeugt Fehler, Skript wird gestoppt!")

    def _create_bon(self):
        pass

    def _create_rechnung(self, bestellung_id: int, rechnungsdatum, summe_netto: float, summe_brutto):
        pass

    def _create_lieferschein(self):
        pass

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