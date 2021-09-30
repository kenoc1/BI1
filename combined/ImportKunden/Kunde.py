from combined.ImportKunden.Adresse import Adresse


class Kunde:
    def __init__(self):
        self.id_combined: int
        self.id_filiale: int
        self.anrede: str
        self.vorname: str
        self.nachname: str
        self.geburtsdatum: str
        self.email: str ='nicht vorhanden'
        self.rechnungsadresse_id: Adresse
        self.lieferadresse_id: Adresse

    def set_vorname(self, vorname):
        self.vorname = vorname

    def set_nachname(self, nachname):
        self.nachname = nachname

    def set_anrede(self, anrede):
        self.anrede = anrede

    def set_rechnungsadresse(self, adresse: Adresse):
        self.rechnungsadresse = adresse

    def set_lieferadresse(self, adresse: Adresse):
        self.lieferadresse = adresse

    def set_email(self, email):
        self.email = email

    def set_geburtsdatum(self, geburtsdatum):
        self.geburtsdatum = geburtsdatum

    def set_id_filiale(self, id):
        self.id_filiale = id

    def set_id_combined(self, id):
        self.id_combined = id






