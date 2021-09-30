import csv

from db_service import DB_F2
import util


class PriceLoader:

    def __init__(self):
        self.db = DB_F2()

    def load_prices_into_database(self):
        with open('../csv-files/produkte.csv', newline='') as csvfile:
            products = csv.reader(csvfile, delimiter=';')
            for product in products:
                product_id = self.db.get_product_id_by_name(productname=product[3])
                if product_id:
                    original_price = float(product[5].replace(",", "."))
                    random_price = util.get_random_price(original_price)
                    self.db.insert_price(betrag=random_price,
                                         beginn='1/1/2019 1:30 PM',
                                         produktid=product_id,
                                         typ='Verkauf')
                    self.db.insert_price(betrag=random_price * 0.8,
                                         beginn='1/1/2019 1:30 PM',
                                         produktid=product_id,
                                         typ='Einkauf')

    def run(self):
        self.load_prices_into_database()
