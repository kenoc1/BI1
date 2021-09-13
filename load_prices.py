from db_interact import DB
import csv
import random

from main import _str_time_prop

db = DB()


def load_prices_into_database():
    with open('produkte.csv', newline='') as csvfile:
        products = csv.reader(csvfile, delimiter=';')
        for product in products:
            product_id = db.get_product_id_by_name(productname=product[3])
            if product_id:
                original_price = float(product[5].replace(",", "."))
                random_price = get_random_price(original_price)
                db.insert_price(betrag=random_price,
                                beginn='1/1/2019 1:30 PM',
                                produktid=product_id,
                                typ='Verkauf')
                db.insert_price(betrag=random_price * 0.8,
                                beginn='1/1/2019 1:30 PM',
                                produktid=product_id,
                                typ='Einkauf')


def get_random_price(original_price: float) -> float:
    return original_price * generate_deviation_factor()


def generate_deviation_factor() -> float:
    return random.uniform(0.8, 1.2)


def random_date(start, end, prop) -> str:
    return _str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)

