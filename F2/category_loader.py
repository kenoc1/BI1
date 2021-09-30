import csv
from db_service import DB_F2


class CategoryLoader:

    def __init__(self):
        self.db = DB_F2()

    @staticmethod
    def load_food_subcategories():
        with open('../csv-files/produkt_kategorien_uebersetzt.csv', newline='') as csvfile:
            categories = csv.reader(csvfile, delimiter=';')
            subcategories = []
            if categories:
                for category in categories:
                    subcategories.append((category[0], category[1]))
        return subcategories

    def get_subcategory_by_excelid(self, excel_id: str) -> str:
        subcats = self.load_food_subcategories()
        for cat in subcats[1:]:
            num = int(cat[0])
            excelnum = int(excel_id)
            if num == excelnum:
                return cat[1]

    def load_category_to_product_into_database(self):
        with open('../csv-files/produkte.csv', newline='') as csvfile:
            products = csv.reader(csvfile, delimiter=';')
            for product in products[1:]:
                product_id = self.db.get_product_id_by_name(product_name=product[3])
                category_name = self.get_subcategory_by_excelid(excel_id=product[0])
                category_id = self.db.get_category_id_by_name(category_name=category_name)
                self.db.insert_product_subcategory(product_id=product_id,
                                                   category_id=category_id)

    def load_category_to_category_into_database(self):
        with open('../csv-files/produkt_kategorien_uebersetzt.csv', newline='') as csvfile:
            categories = csv.reader(csvfile, delimiter=';')
            for category in categories:
                category_id = self.db.get_category_id_by_name(category_name=category[1])
                if category[4] in ["Food", "Drink"]:
                    self.db.insert_oberkategorie_subcategory(subcat_id=category_id,
                                                             ocat_id=1)
                else:
                    self.db.insert_oberkategorie_subcategory(subcat_id=category_id,
                                                             ocat_id=2)

    def load_subcategory_csv_into_database(self):
        with open('../csv-files/produkt_kategorien_uebersetzt.csv', newline='') as csvfile:
            categories = csv.reader(csvfile, delimiter=';')
            for category in categories:
                if category[4] in ['Food', 'Drink'] or category[2] in ['Elektrisch']:
                    self.db.insert_subcategory(description=category[1],
                                               age_restriction=(16 if category[3] in ['Alkoholische Getränke'] else 0))

    def run(self):
        # Oberkategorien müssen bereits händisch eingefügt worden sein
        self.load_subcategory_csv_into_database()
        self.load_category_to_category_into_database()
        self.load_category_to_product_into_database()
