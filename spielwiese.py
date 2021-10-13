import sys

import cx_Oracle

from db_service import DB_F2, DB_MASTER
from rudolf import config
from rudolf.sqlite_service import SQLiteService

con_comb = cx_Oracle.connect(user=config.DB_CON_USER_COMBINED, password=config.DB_CON_PW_COMBINED,
                             dsn=config.DB_CON_DSN_F2,
                             encoding="UTF-8")


def _select_bon_join(con) -> list[dict]:
    with con.cursor() as cursor:
        cursor.execute(
            "SELECT BESTELLUNG.BESTELLUNG_ID FROM BESTELLUNG,BON WHERE BON.BESTELLUNG_ID = BESTELLUNG.BESTELLUNG_ID ")
        cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
        rows = cursor.fetchall()
        if rows:
            return rows
        else:
            return []


def _select_rechnung_join(con) -> list[dict]:
    with con.cursor() as cursor:
        cursor.execute(
            "SELECT BESTELLUNG.BESTELLUNG_ID FROM BESTELLUNG,RECHNUNG WHERE RECHNUNG.BESTELLUNG_ID = BESTELLUNG.BESTELLUNG_ID ")
        cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
        rows = cursor.fetchall()
        if rows:
            return rows
        else:
            return []


for bon_join in _select_bon_join(con_comb):
    print(bon_join.get("BESTELLUNG_ID"))
    with con_comb.cursor() as cursor:
        cursor.callproc('correct_bestellpositionen', [bon_join.get("BESTELLUNG_ID")])
        cursor.callproc('correct_bestell_bons', [bon_join.get("BESTELLUNG_ID")])

for rechnung_join in _select_rechnung_join(con_comb):
    print(rechnung_join.get("BESTELLUNG_ID"))
    with con_comb.cursor() as cursor:
        cursor.callproc('correct_bestellpositionen', [rechnung_join.get("BESTELLUNG_ID")])
        cursor.callproc('correct_bestell_rechungen', [rechnung_join.get("BESTELLUNG_ID")])

#  -------- FELIX^ ------------

con_f2 = DB_F2()
con_comb = DB_MASTER()

f2_rechnungsdaten: list[dict] = con_f2.select_all_rechnung()
f2_bondaten: list[dict] = con_f2.select_all_bons()
f2_lieferschein: list[dict] = con_f2.select_all_lieferschein()
all_f2_bestellungen = con_f2.select_all_verkauf()

sqlite_service = SQLiteService()
for f2_bestellung in all_f2_bestellungen:
    sqlite_service.select_where_old_id(config.BESTELLUNG_TABLE, f2_bestellung.get("VERKAUFS_ID"))


def _create_verkaufsdokumente(new_bestellung_id: int) -> None:
    bon_element: dict = next((elem for elem in f2_bondaten if
                              sqlite_service.select_where_old_id(config.BESTELLUNG_TABLE,
                                                                 elem.get("VERKAUFS_ID")) == new_bestellung_id), None)
    if bon_element:
        _create_bon(bon_element, new_bestellung_id)
        con_comb.insert_bestellung_to_zahlungsart(bestellungid=new_bestellung_id,
                                                  zahlungsart_id=41 if bon_element.get(
                                                      "ZAHLUNGSART") == "Bar" else 3)
    rechnung_element: dict = next((elem for elem in f2_rechnungsdaten if
                                   sqlite_service.select_where_old_id(config.BESTELLUNG_TABLE,
                                                                      elem.get("VERKAUFS_ID")) == new_bestellung_id),
                                  None)

    if rechnung_element:
        _create_rechnung(rechnung_element, new_bestellung_id)
        con_comb.insert_bestellung_to_zahlungsart(bestellungid=new_bestellung_id,
                                                  zahlungsart_id=2)

    lieferschein_element: dict = next((elem for elem in f2_lieferschein if
                                       sqlite_service.select_where_old_id(config.BESTELLUNG_TABLE, elem.get(
                                           "VERKAUFS_ID")) == new_bestellung_id), None)
    if lieferschein_element:
        _create_lieferschein(lieferschein_element, new_bestellung_id)


def _create_bon(bon: dict, bestellung_id: int) -> int:
    return con_comb.insert_bon(bestellungid=bestellung_id, gegebenesgeld=bon.get("GEGEBENES_GELD"),
                               rueckgeld=bon.get("RUECKGELD"), summe_netto=0, summe_brutto=0)


def _create_rechnung(rechnung: dict, bestellung_id: int) -> int:
    return con_comb.insert_rechnung(bestellungid=bestellung_id,
                                    rechnungsdatum=rechnung.get("ABGLEICH_DATUM"),
                                    summe_netto=0, summe_brutto=0)


def _create_lieferschein(lieferschein: dict, bestellung_id: int):
    return con_comb.insert_lieferschein(bestellungid=bestellung_id,
                                        lieferdatum=lieferschein.get("LIEFERDATUM"),
                                        lieferkosten=lieferschein.get("LIEFER_KOSTEN"))
