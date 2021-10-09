from rudolf.etl_scripts.adresse import Adresse
from rudolf.etl_scripts.bestellung import Bestellung
from rudolf.etl_scripts.einkauf import Einkauf
from rudolf.etl_scripts.kunde import Kunde
from rudolf.etl_scripts.lagerplatz import Lagerplatz
from rudolf.etl_scripts.marke import Marke
from rudolf.etl_scripts.mitarbeiter import Mitarbeiter
from rudolf.etl_scripts.mitarbeiter_funktion import MitarbeiterFunktion
from rudolf.etl_scripts.preishistorie import Preishistorie
from rudolf.etl_scripts.produkt import Produkt
from rudolf.etl_scripts.zwischenhaendler import Zwischenhaendler


class ETLPipeline:

    def __init__(self):
        self._progress_list = [{"name": "Adressen", "status": 0},
                               {"name": "Funktionen", "status": 0},
                               {"name": "Mitarbeiter", "status": 0},
                               {"name": "Kunden", "status": 0},
                               {"name": "Marken", "status": 0},
                               {"name": "Produkte", "status": 0},
                               {"name": "Preishistorien", "status": 0},
                               {"name": "Lagerplatz", "status": 0},
                               {"name": "Zwischenhändler", "status": 0},
                               {"name": "Einkauf", "status": 0},
                               {"name": "Verkauf", "status": 0}
                               ]

    @property
    def progress_list(self):
        return self._progress_list

    def start_adresse(self):
        address = Adresse()
        address.init()
        address.start()
        next(elem.update({"name": "Adressen", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Adressen")
        next(elem.update({"name": "Funktionen", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Funktionen")

    def start_funktion(self):
        funktion = MitarbeiterFunktion()
        funktion.init()
        funktion.start()
        next(elem.update({"name": "Funktionen", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Funktionen")
        next(elem.update({"name": "Mitarbeiter", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Mitarbeiter")

    def start_mitarbeiter(self):
        mitarbeiter = Mitarbeiter()
        mitarbeiter.init()
        mitarbeiter.start()
        next(elem.update({"name": "Mitarbeiter", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Mitarbeiter")
        next(elem.update({"name": "Kunden", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Kunden")

    def start_kunde(self):
        kunde = Kunde()
        kunde.init()
        kunde.start()
        next(elem.update({"name": "Kunden", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Kunden")
        next(elem.update({"name": "Marken", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Marken")

    def start_marke(self):
        marke = Marke()
        marke.init()
        marke.start()
        next(elem.update({"name": "Marken", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Marken")
        next(elem.update({"name": "Produkte", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Produkte")

    def start_produkt(self):
        produkt = Produkt()
        produkt.init()
        produkt.start()
        next(elem.update({"name": "Produkte", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Produkte")
        next(elem.update({"name": "Preishistorien", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Preishistorien")

    def start_preishistorie(self):
        preishistorie = Preishistorie()
        preishistorie.init()
        preishistorie.start()
        next(elem.update({"name": "Preishistorien", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Preishistorien")
        next(elem.update({"name": "Lagerplatz", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Lagerplatz")

    def start_lagerplatz(self):
        lagerplatz = Lagerplatz()
        lagerplatz.init()
        lagerplatz.start()
        next(elem.update({"name": "Lagerplatz", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Lagerplatz")
        next(elem.update({"name": "Zwischenhändler", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Zwischenhändler")

    def start_zwhaendler(self):
        zwischenhaendler = Zwischenhaendler()
        zwischenhaendler.init()
        zwischenhaendler.start()
        next(elem.update({"name": "Zwischenhändler", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Zwischenhändler")
        next(elem.update({"name": "Einkauf", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Einkauf")

    def start_einkauf(self):
        einkauf = Einkauf()
        einkauf.init()
        einkauf.start()
        next(elem.update({"name": "Einkauf", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Einkauf")
        next(elem.update({"name": "Verkauf", "status": 1}) for elem in self.progress_list if
             elem.get("name") == "Verkauf")

    def start_bestellung(self):
        bestellung = Bestellung()
        bestellung.init()
        bestellung.start()
        next(elem.update({"name": "Verkauf", "status": 2}) for elem in self.progress_list if
             elem.get("name") == "Verkauf")

    def start(self):
        pass
        # start_adresse
        # start_funktion
        # start_mitarbeiter
        # Lager # MANUELL (muss eigentlich nicht wirklich)
        # Kategorien # MANUELL
        # start_kunde
        # Zahlungsart MANUELL
        # start_marke
        # start_produkt
        # start_preishistorie
        # start_lagerplatz
        # start_zwhaendler
        # start_einkauf
        # start_bestellung
