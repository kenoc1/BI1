from rudolf.etl_scripts.address import Address
from rudolf.etl_scripts.bestellung import Bestellung
from rudolf.etl_scripts.customer import Customer
from rudolf.etl_scripts.lagerplatz import Lagerplatz
from rudolf.etl_scripts.worker import Worker
from rudolf.etl_scripts.worker_positions import WorkerPositions


class ETLPipeline:

    def __init__(self):
        pass

    def start(self):
        # Adresse
        address = Address()
        address.init()
        address.start()
        # Funktion
        WorkerPositions().start()
        # Mitarbeiter
        Worker().start()

        # Lager # MANUELL
        # Kategorien # MANUELL

        # Kunde
        Customer().start()

        # Zahlungsart MANUELL

        # Produkt
        # TODO Produkt

        # Lagerplatz
        Lagerplatz().start()

        # Zwischenhaendler
        # TODO Zwischenhaendler

        # Bestellung
        bestellung = Bestellung()
        bestellung.init()
        bestellung.start()
