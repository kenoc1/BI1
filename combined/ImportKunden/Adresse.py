class Adresse:
    def __init__(self):
        self.land: str
        self.plz: int
        self.ort: str
        self.strasse: str
        self.nummer: str
        self.art: str

    def set_land(self, land):
        self.land = land

    def set_plz(self, plz):
        self.plz = plz

    def set_plz(self, ort):
        self.ort = ort

    def set_ort(self, strasse):
        self.strasse = strasse

    def set_strasse(self, nummer):
        self.nummer = nummer

    def set_art(self, art):
        self.art = art

