import logging
from datetime import datetime
import csv
from db_interact import DB

logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)

# logging.debug(datetime.now(), 'This message should go to the log file')
# logging.info(datetime.now(), 'So should this')
# logging.warning(datetime.now(), 'And this, too')
logging.error(datetime.now().strftime(" %H:%M:%S.%f - %d/%b/%Y ") + '')

db = DB()


def load_product_dimensions_into_database():
    with open('produkte.csv', newline='') as csvfile:
        products = csv.reader(csvfile, delimiter=';')
        for product in products:
            product_id = db.get_product_id_by_name(product[3])
            if product_id:
                db.insert_dimension(cm_to_inch(weight_to_correct_float(product[12])),
                                    cm_to_inch(weight_to_correct_float(product[13])),
                                    cm_to_inch(weight_to_correct_float(product[14])),
                                    product_id)


def cm_to_inch(cm: float) -> float:
    return cm / 2.54


def weight_to_correct_float(value: str) -> float:
    a = value.replace(",", ".")
    b = float(a)
    c = round(b, 4)
    return c


load_product_dimensions_into_database()
