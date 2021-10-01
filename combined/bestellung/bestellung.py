import sys

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
        self.comb_warenkoerbe: list[dict] = self.combined_con.select_all_warenkoerbe()
        self.f2_gewichtsbasiert_im_verkauf = self.f2_con.select_all_gewichtsbasiert_verkauf()
        self.f2_stueckzahlbasiert_im_verkauf = self.f2_con.select_all_stueckzahlbasiert_verkauf()

    def _verkauf_to_bestellung(self):
        try:
            f2_sales: list[dict] = self.f2_con.select_all_sales()
            for sale in f2_sales:
                # TODO Preise umrechnen
                new_bestellung_id: int = self._create_bestellung(f2_verkauf_entry=sale)
                self._create_verkaufsdokumente(new_bestellung_id=new_bestellung_id)
                self._create_bestellposition(new_bestellung_id=new_bestellung_id, verkauf=sale)
        except cx_Oracle.Error as error:
            print('Database error occurred:')
            print(error)
            sys.exit("Datenbankverbindung erzeugt Fehler, Skript wird gestoppt!")

    # def _get_warenkorb(self, kunden_id: int) -> list[dict]:
    #     try:
    #         return self.combined_con.select_warenkorb_by_kundenid(kunden_id=kunden_id)
    #     except cx_Oracle.Error as error:
    #         print('Database error occurred:')
    #         print(error)
    #         sys.exit("Datenbankverbindung erzeugt Fehler, Skript wird gestoppt!")

    def _create_bestellung(self, f2_verkauf_entry: dict):
        verkaufsdatum = f2_verkauf_entry.get("VERKAUFDATUM")
        f2_kunden_id: int = f2_verkauf_entry.get("KUNDEN_ID")
        com_kunden_id: int = self._get_com_kundenid_by_f2_kundenid(
            f2_kunden_id) if f2_kunden_id else self.get_dummy_kunde_id()
        f2_mitarbeiter_id: int = f2_verkauf_entry.get("MITARBEITER_ID")
        com_mitarbeiter_id: int = self._get_com_mitarbeiterid_by_f2_mitarbeiterid(f2_mitarbeiter_id)
        # warenkorb_id: int = self._get_warenkorb(com_kunden_id).__getitem__(0).get("WARENKORB_ID")
        warenkorb_id: int = next((elem.get("WARENKORB_ID") for elem in self.comb_warenkoerbe if
                                  elem.get("KUNDE_ID") == com_kunden_id), None)
        new_bestellung_id: int = self.combined_con.insert_bestellung(warenkorb_id=warenkorb_id,
                                                                     status="Abgeschlossen",
                                                                     bestelldatum=verkaufsdatum,
                                                                     datenherkunft=2,
                                                                     mitarbeiter_id=com_mitarbeiter_id)
        file_writer.write_to_csv([], [new_bestellung_id, f2_verkauf_entry.get("VERKAUFS_ID")],
                                 "bestellung_ids_new_to_old.csv")
        return new_bestellung_id

    def _create_verkaufsdokumente(self, new_bestellung_id: int):
        #  TODO neue und alte IDs speichern
        bon_element: dict = next((elem.get("VERKAUFS_ID") for elem in self.f2_bondaten if
                                  elem.get("VERKAUFS_ID") == new_bestellung_id), None)
        if bon_element:
            comb_bon_id: int = self._create_bon(bon_element, new_bestellung_id)
            file_writer.write_to_csv([], [comb_bon_id, bon_element.get("BON_NUMMER")], "bon_ids_new_to_old.csv")
            self.combined_con.insert_bestellung_to_zahlungsart(bestellungid=new_bestellung_id,
                                                               zahlungsart_id=41 if bon_element.get(
                                                                   "ZAHLUNGSART") == "Bar" else 3)
        rechnung_element: dict = next((elem.get("VERKAUFS_ID") for elem in self.f2_rechnungsdaten if
                                       elem.get("VERKAUFS_ID") == new_bestellung_id), None)

        if rechnung_element:
            comb_rechnung_id: int = self._create_rechnung(rechnung_element, new_bestellung_id)
            file_writer.write_to_csv([], [comb_rechnung_id, rechnung_element.get("RECHNUNG_NUMMER")],
                                     "rechnung_ids_new_to_old.csv")
            self.combined_con.insert_bestellung_to_zahlungsart(bestellungid=new_bestellung_id,
                                                               zahlungsart_id=2)

        lieferschein_element: dict = next((elem.get("VERKAUFS_ID") for elem in self.f2_lieferschein if
                                           elem.get("VERKAUFS_ID") == new_bestellung_id), None)
        if lieferschein_element:
            comb_lieferschein_id: int = self._create_lieferschein(lieferschein_element, new_bestellung_id)
            file_writer.write_to_csv([], [comb_lieferschein_id, lieferschein_element.get("LIEFERSCHEIN_NUMMER")],
                                     "lieferschein_ids_new_to_old.csv")

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
        # TODO umrechnen
        [eintrag.update({"ANZAHL_MENGE": self._calculate_menge_from_gewicht(eintrag.get("GEWICHT"))}) for eintrag in
         gewichtsbasierte_produkte]
        stueckbasierte_produkte: list[dict] = [elem for elem in self.f2_stueckzahlbasiert_im_verkauf if
                                               elem.get("VERKAUFS_ID") == verkauf.get("VERKAUFS_ID")]
        for produkt_im_verkauf in gewichtsbasierte_produkte + stueckbasierte_produkte:
            self.combined_con.insert_bestellposition(bestellungid=new_bestellung_id,
                                                     produktid=self._get_new_productid(
                                                         produkt_im_verkauf.get("PRODUKT_ID")),
                                                     menge=produkt_im_verkauf.get("ANZAHL_MENGE"))

    def _calculate_menge_from_gewicht(self, menge: float) -> int:
        pass

    def _get_new_productid(self, old_id: int) -> int:
        # TODO neue produktid holen
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
