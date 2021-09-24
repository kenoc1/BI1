import csv

from db_interact import DB
from util import Util

db = DB()


class PriceLoader:

    def __init__(self):
        self.db = DB
        self.util = Util

    def load_prices_into_database(self):
        with open('produkte.csv', newline='') as csvfile:
            products = csv.reader(csvfile, delimiter=';')
            for product in products:
                product_id = db.get_product_id_by_name(productname=product[3])
                if product_id:
                    original_price = float(product[5].replace(",", "."))
                    random_price = self.util.get_random_price(original_price)
                    db.insert_price(betrag=random_price,
                                    beginn='1/1/2019 1:30 PM',
                                    produktid=product_id,
                                    typ='Verkauf')
                    db.insert_price(betrag=random_price * 0.8,
                                    beginn='1/1/2019 1:30 PM',
                                    produktid=product_id,
                                    typ='Einkauf')

    def run(self):
        self.load_prices_into_database()
