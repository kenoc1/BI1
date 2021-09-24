import csv
from random import random, randint, uniform

import util
from db_service import DB_F2

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
        counter = 0
        for row in retail_reader:
            counter += 1
            print(counter)
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
                    if count_products:
                        for product in count_products:
                            weight_sum += product["BRUTTOGEWICHT"]
                            price = db.select_current_sale_price_with_product_id(product["PRODUKT_ID"])
                            sale_sum += price["BETRAG"]
                            tax_sum += price["BETRAG"] * product["UMSATZSTEUERSATZ"] * 0.01
                    if weight_products:
                        for product in weight_products:
                            # print(product)
                            count = uniform(0.1, 3)
                            product["PURCHASED_WEIGHT"] = product["NETTOGEWICHT"] * count
                            weight_sum += product["NETTOGEWICHT"] * count
                            price = db.select_current_sale_price_with_product_id(product["PRODUKT_ID"])
                            sale_sum += price["BETRAG"] * count
                            tax_sum += price["BETRAG"] * product["UMSATZSTEUERSATZ"] * 0.01 * count

                    worker_id = db.get_random_seller()
                    customer_id = db.get_random_customer()
                    random_date_indicator = randint(1, 5)
                    # 1 month
                    if random_date_indicator == 3:
                        sale_date = util.random_date_for_priceloader('12/15/2020 1:30 PM', '1/15/2021 4:50 AM', random())
                    # 1 month
                    elif random_date_indicator == 2:
                        sale_date = util.random_date_for_priceloader('6/1/2021 1:30 PM', '6/30/2021 4:50 AM', random())
                    # 3 month (execute if 1,4,5)
                    else:
                        sale_date = util.random_date_for_priceloader('8/20/2021 1:30 PM', '11/30/2021 4:50 AM', random())

                    added_sale_id = db.insert_verkauf_row(
                        sale_date=sale_date, worker_id=worker_id, customer_id=customer_id, sale_netto_sum=sale_sum,
                        sale_brutto_sum=sale_sum + tax_sum, tax_sum=tax_sum, weight_sum=weight_sum)
                    db.calculate_and_insert_count_products_verkauf(added_sale_id, count_products)
                    db.calculate_and_insert_weight_products_verkauf(added_sale_id, weight_products)
                    if randint(0, 100) < 30:
                        db.insert_delivery_note_row(sale_id=added_sale_id,
                                                    delivery_date=util.generate_delivery_date(sale_date),
                                                    delivery_costs=util.generate_delivery_costs())
                        db.insert_invoice_row(sale_id=added_sale_id,
                                              adjustment_date=util.generate_adjustment_date(sale_date))
                    else:
                        db.insert_bon_row(sale_id=added_sale_id,
                                          given_money=util.generate_given_money(sale_sum + tax_sum),
                                          payment_method=util.generate_payment_method(),
                                          sale_sum=sale_sum + tax_sum)


def import_retail_einkauf():
    with open('retail1.csv', newline='') as csv_file:
        retail_reader = csv.reader(csv_file, delimiter=' ', quotechar='|')
        counter = 0
        for row in retail_reader:
            counter += 1
            print(counter)
            if row:
                print(row)
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
                    if weight_products:
                        for product in weight_products:
                            count = uniform(3, 7)
                            product["PURCHASED_WEIGHT"] = product["NETTOGEWICHT"] * count
                worker_id = db.get_random_buyer()
                supplier_id = db.get_random_supplier()

                added_buying_id = db.insert_einkauf_row(
                    util.random_date_for_priceloader('1/1/2017 1:30 PM', '1/1/2019 4:50 AM', random()),
                    worker_id, supplier_id)
                db.calculate_and_insert_count_products_einkauf(added_buying_id, count_products)
                db.calculate_and_insert_weight_products_einkauf(added_buying_id, weight_products)


db = DB_F2()
# x = db.select_current_sale_price_with_product_id(2324)
# y = db.select_current_buying_price_with_product_id(2324)
# print(x["BETRAG"])
import_retail_verkauf()
# import_retail_einkauf()
