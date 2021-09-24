import csv
import util
from db_service import DB_F2


class ProductLoader:
    weight_based_categories = [24, 13, 61, 77, 99]

    def __init__(self):
        self.db = DB_F2()

    @staticmethod
    def load_food_categories():
        food_categories = []
        with open('../csv-files/produkt_kategorien.csv', newline='') as csvfile:
            categories = csv.reader(csvfile, delimiter=';')
            if categories:
                for category in categories:
                    if category[4] in ["Food", "Drink"]:
                        food_categories.append(category[0])
        return food_categories

    def load_product_csv_into_database(self):
        food_categories = self.load_food_categories()
        with open('../csv-files/produkte.csv', newline='') as csvfile:
            products = csv.reader(csvfile, delimiter=';')
            for product in products:
                if product[0] in food_categories:
                    self.db.insert_product(nettogewicht=util.oz_to_ibs(util.number_str_to_float(product[7])),
                                           umsatzsteuer=7.00,
                                           bezeichnung=product[3],
                                           sku=int(product[4]),
                                           art=('gewichtsbasiert' if int(
                                               product[0]) in self.weight_based_categories else 'stueckbasiert'),
                                           marke=self.db.select_brand_id(product[2]),
                                           bruttogewicht=util.oz_to_ibs(util.number_str_to_float(product[6])),
                                           pwidth=util.cm_to_inch(util.number_str_to_float(product[12])),
                                           pheight=util.cm_to_inch(util.number_str_to_float(product[13])),
                                           pdepth=util.cm_to_inch(util.number_str_to_float(product[14])))

    def run(self):
        self.load_product_csv_into_database()
