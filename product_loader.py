import csv
from util import Util
from db_interact import DB


class ProductLoader:

    weight_based_categories = [24, 13, 61, 77, 99]

    def __init__(self):
        self.db = DB()

    @staticmethod
    def load_food_categories():
        food_categories = []
        with open('produkt_kategorien.csv', newline='') as csvfile:
            categories = csv.reader(csvfile, delimiter=';')
            if categories:
                for category in categories:
                    if category[4] in ["Food", "Drink"]:
                        food_categories.append(category[0])
        return food_categories

    def load_product_csv_into_database(self):
        food_categories = self.load_food_categories()
        with open('produkte.csv', newline='') as csvfile:
            products = csv.reader(csvfile, delimiter=';')
            for product in products:
                if product[0] in food_categories:
                    self.db.insert_product(nettogewicht=Util.oz_to_ibs(Util.number_str_to_float(product[7])),
                                           umsatzsteuer=7.00,
                                           bezeichnung=product[3],
                                           sku=int(product[4]),
                                           art=('gewichtsbasiert' if int(
                                               product[0]) in self.weight_based_categories else 'stueckbasiert'),
                                           marke=self.db.select_brand_id(product[2]),
                                           bruttogewicht=Util.oz_to_ibs(Util.number_str_to_float(product[6])),
                                           pwidth=Util.cm_to_inch(Util.number_str_to_float(product[12])),
                                           pheight=Util.cm_to_inch(Util.number_str_to_float(product[13])),
                                           pdepth=Util.cm_to_inch(Util.number_str_to_float(product[14])))

    def run(self):
        self.load_product_csv_into_database()
