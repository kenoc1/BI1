from rudolf.etl_scripts.address import Address
from rudolf.etl_scripts.customer import Customer
from rudolf.etl_scripts.lagerplatz import Lagerplatz
from rudolf.etl_scripts.worker import Worker
from rudolf.etl_scripts.worker_positions import WorkerPositions


class ETLPipeline:

    def __init__(self):
        pass

    def start(self):
        Address().start()
        WorkerPositions().start()
        Worker().start()
        # Lager # MANUELL
        # TODO Kategorien # MANUELL
        Customer().start()
        # Zahlungsart MANUELL
        # TODO Produkt
        Lagerplatz().start()
        # TODO Zwischenhaendler
        # TODO Bestellung
