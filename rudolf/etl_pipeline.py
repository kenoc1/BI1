from rudolf.etl_scripts.adresse import Adresse
from rudolf.etl_scripts.bestellung import Bestellung
from rudolf.etl_scripts.kunde import Kunde
from rudolf.etl_scripts.lagerplatz import Lagerplatz
from rudolf.etl_scripts.marke import Marke
from rudolf.etl_scripts.mitarbeiter import Mitarbeiter
from rudolf.etl_scripts.mitarbeiter_funktion import MitarbeiterFunktion
from rudolf.etl_scripts.preishistorie import Preishistorie
from rudolf.etl_scripts.produkt import Produkt


class ETLPipeline:

    def __init__(self):
        pass

    def start(self):
        # Adresse
        address = Adresse()
        address.init()
        address.start()
        # Funktion
        funktion = MitarbeiterFunktion()
        funktion.init()
        funktion.start()
        # Mitarbeiter
        mitarbeiter = Mitarbeiter()
        mitarbeiter.init()
        mitarbeiter.start()

        # Lager # MANUELL
        # Kategorien # MANUELL

        # Kunde
        kunde = Kunde()
        kunde.init()
        kunde.start()

        # Zahlungsart MANUELL

        # Marke
        marke = Marke()
        marke.init()
        marke.start()

        # Produkt
        produkt = Produkt()
        produkt.init()
        produkt.start()

        # Preishistorie
        preishistorie = Preishistorie()
        preishistorie.init()
        preishistorie.start()

        # Lagerplatz
        lagerplatz = Lagerplatz()
        lagerplatz.init()
        lagerplatz.start()

        # Zwischenhaendler
        # TODO Zwischenhaendler

        # Bestellung
        bestellung = Bestellung()
        bestellung.init()
        bestellung.start()
