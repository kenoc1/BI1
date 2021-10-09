import socketio

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
        self.sio = socketio.Client()

    def connect(self):
        self.sio.connect('http://localhost:8080')

    def test(self):
        self.sio.emit('update_progress', {"name": "Adressen", "status": 0})

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

        # Lager # MANUELL (muss eigentlich nicht wirklich)
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
        zwischenhaendler = Zwischenhaendler()
        zwischenhaendler.init()
        zwischenhaendler.start()

        # Einkauf
        einkauf = Einkauf()
        einkauf.init()
        einkauf.start()

        # Bestellung
        bestellung = Bestellung()
        bestellung.init()
        bestellung.start()

    def _send_progress_update(self, data):
        self.socket_client.emit('update_progress', data)
