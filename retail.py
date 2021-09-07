import csv
import time
from random import random, seed, gauss, randint, uniform
import config
import cx_Oracle
import names
from collections import Counter

from db_service import DB
import util

db = None


# VERKAUF:
# VERKAUFS_ID
# VERKAUFDATUM
# NETTOSUMME
# BRUTTOSUMME
# STEUERSUMME
# MITARBEITER_ID
# KUNDEN_ID

# STUECKZAHLBASIERTES_PRODUKT_IM_VERKAUF:
# VERKAUFS_ID
# PRODUKT_ID
# ANZAHL_PRODUKTE

# GEWICHTBASIERTES_PRODUKT_IM_VERKAUF:
# VERKAUFS_ID
# PRODUKT_ID
# GEWICHT

# PRODUKT:
# PRODUKT_ID
# UMSATZSTEUERSATZ
# SKU
# TYP ('gewichtsbasiert', 'stueckbasiert')
# BEZEICHNUNG
# NETTOGEWICHT
# BRUTTOGEWICHT
# UMSATZSTEUERSATZ
# MARKE_ID
# PREIS_EINKAUF_ID
# PREIS_VERKAUF_ID


def import_retail_verkauf():
    with open('retail.csv', newline='') as csv_file:
        retail_reader = csv.reader(csv_file, delimiter=' ', quotechar='|')
        for row in retail_reader:
            # print(row)
            if row:
                weight_products = []
                count_products = []
                for product_id in row:
                    if db.product_present(product_id):
                        product = db.select_product_with_id(product_id)
                        if product["TYP"] == 'stueckbasiert':
                            count_products.append(product)
                        elif product["TYP"] == 'gewichtsbasiert':
                            weight_products.append(product)
                if count_products or weight_products:
                    tax_sum = 0
                    sale_sum = 0
                    weight_sum = 0
                    # count_products_ids = []
                    # weight_products_ids = []
                    if count_products:
                        for product in count_products:
                            # ToDo: get Preis and add tax and sale sum
                            # print(product)
                            weight_sum += product["BRUTTOGEWICHT"]
                    if weight_products:
                        for product in weight_products:
                            # ToDo: get Preis and add tax and sale sum
                            # print(product)
                            count = uniform(0.1, 3)
                            product["PURCHASED_WEIGHT"] = product["NETTOGEWICHT"] * count
                            weight_sum += product["BRUTTOGEWICHT"] * count

                    worker_id = db.get_random_seller()
                    customer_id = db.get_random_customer()

                    added_sale_id = db.insert_verkauf_row(
                        sale_date=util.random_date('1/1/2019 1:30 PM', '1/1/2021 4:50 AM', random()),
                        worker_id=worker_id, customer_id=customer_id, sale_sum=sale_sum, tax_sum=tax_sum,
                        weight_sum=weight_sum)
                    calculate_and_insert_count_products(added_sale_id, count_products)
                    calculate_and_insert_weight_products(added_sale_id, weight_products)


def import_retail_einkauf():
    with open('retail.csv', newline='') as csv_file:
        retail_reader = csv.reader(csv_file, delimiter=' ', quotechar='|')
        for row in retail_reader:
            if row:
                print(row)
                weight_products = []
                count_products = []
                for product_id in row:
                    if db.product_present(product_id):
                        product = db.select_product_with_id(product_id)
                        if product["TYP"] == 'stueckbasiert':
                            # ToDo: Add random count products
                            count_products.append(product)
                        elif product["TYP"] == 'gewichtsbasiert':
                            weight_products.append(product)
                if count_products or weight_products:
                    sale_sum = 0
                    if count_products:
                        for product in count_products:
                            # ToDo: get sale sum
                            print(product)
                    if weight_products:
                        for product in weight_products:
                            # ToDo: get sale sum
                            print(product)
                            count = uniform(3, 7)
                            product["PURCHASED_WEIGHT"] = product["NETTOGEWICHT"] * count
                worker_id = db.get_random_buyer()
                customer_id = db.get_random_customer()

                added_buying_id = db.insert_einkauf_row()
                calculate_and_insert_count_products(added_buying_id, count_products)
                calculate_and_insert_weight_products(added_buying_id, weight_products)


def calculate_and_insert_count_products(sale_id, count_products):
    count_products_ids = []
    if count_products:
        tmp_product_ids = []
        for product in count_products:
            tmp_product_ids.append(product["PRODUKT_ID"])
        tmp_product_ids_counter = Counter(tmp_product_ids)
        for product_id in tmp_product_ids_counter:
            count_products_ids.append(
                [sale_id, product_id, tmp_product_ids_counter[product_id]])
        db.insert_verkauf_stueckbasiert(count_products_ids)


def calculate_and_insert_weight_products(sale_id, weight_products):
    weight_products_ids = []
    if weight_products:
        for product in weight_products:
            weight_products_ids.append(
                [sale_id, product["PRODUKT_ID"], product["PURCHASED_WEIGHT"]])
        db.insert_verkauf_gewichtsbasiert(weight_products_ids)


db = DB()
import_retail_verkauf()
# import_retail_einkauf()
