from datetime import datetime
import config
import pandas as pd
from db_service import DB


def print_error():
    print()
    print("ERROR!!!")
    print()


def print_menu():
    print()
    print("-----MENUE-----")
    print("1 - Kasse")
    print("2 - Einkauf")
    print("3 - Kunden anzeigen")
    print("4 - Mitarbeiter anzeigen")
    print("5 - Mitarbeiter Funktionen anzeigen")
    print("6 - Lieferscheine anzeigen")
    print("7 - Rechnungen anzeigen")
    print("8 - Bons anzeigen")


def print_menu_kasse():
    print()
    print("-----Kasse-----")
    print("1 - Neuer Einkauf")
    print("2 - Produkte suchen")


class Terminal():
    def __init__(self):
        self.db = DB()

    def start(self):
        while True:
            print_menu()
            selected_point = int(input("Auswahl: "))
            if selected_point == 1:
                print_menu_kasse()
                selected_point_checkout = int(input("Auswahl: "))
                if selected_point_checkout == 1:
                    sale_date = datetime.now().strftime("%m/%d/%Y %I:%M %p")
                    worker_id = int(input("Mitarbeiter ID: "))
                    customer_id = int(input("Kunden ID: "))

                    more_products = "y"
                    tax_sum = 0
                    sale_sum = 0
                    weight_sum = 0
                    weight_products = []
                    count_products = []
                    while more_products != "n":
                        print()
                        product_id = int(input("Produkt ID: "))
                        product = self.db.select_product_with_id(product_id)
                        if product["TYP"] == "gewichtsbasiert":
                            product_weight = float(input("Gewicht Produkt: "))
                            weight_products.append(product)
                            product["PURCHASED_WEIGHT"] = product_weight
                            weight_sum += float(product_weight * product["BRUTTOGEWICHT"] / product["NETTOGEWICHT"])
                            price = self.db.select_current_sale_price_with_product_id(product["PRODUKT_ID"])
                            sale_sum += float(price["BETRAG"] * product_weight / product["NETTOGEWICHT"])
                            tax_sum += float((price["BETRAG"] * product_weight / product["NETTOGEWICHT"]) * product[
                                "UMSATZSTEUERSATZ"] * 0.01)
                        elif product["TYP"] == "stueckbasiert":
                            product_count = float(input("Anzahl Produkt: "))
                            count_products.append(product)
                            weight_sum += float(product["BRUTTOGEWICHT"] * product_count)
                            price = self.db.select_current_sale_price_with_product_id(product["PRODUKT_ID"])
                            sale_sum += float(price["BETRAG"] * product_count)
                            tax_sum += float(price["BETRAG"] * product["UMSATZSTEUERSATZ"] * 0.01 * product_count)
                        else:
                            print_error()
                            return
                        more_products = input("mehr Produkte einfügen (ja-ENTER: nein-n)")

                    payment_method = int(input("Auswahl (0-bar; 1-karte): "))
                    given_money = 0
                    if payment_method == 0:
                        given_money = float(input("Gegebenes Geld (IB$): "))

                    added_sale_id = self.db.insert_verkauf_row(
                        sale_date=sale_date, worker_id=worker_id, customer_id=customer_id, sale_netto_sum=sale_sum,
                        sale_brutto_sum=sale_sum + tax_sum, tax_sum=tax_sum, weight_sum=weight_sum)
                    self.db.calculate_and_insert_count_products_verkauf(added_sale_id, count_products)
                    self.db.calculate_and_insert_weight_products_verkauf(added_sale_id, weight_products)
                    change = self.db.insert_bon_row(sale_id=added_sale_id,
                                                    given_money=given_money,
                                                    payment_method=config.PAYMENT_METHODS[payment_method],
                                                    sale_sum=sale_sum + tax_sum)
                    print("Rückgeld : " + change + " (IB$)")
                elif selected_point_checkout == 2:
                    product_description = input("Produkt Beschreibung: ")
                    print(f"-----Produkte mit: <{product_description}>-----")
                    df_products = pd.DataFrame(self.db.select_product_with_description(product_description))
                    print(df_products)
                else:
                    print_error()
                    return
            elif selected_point == 2:
                buying_date = datetime.now().strftime("%m/%d/%Y %I:%M %p")
                worker_id = int(input("Mitarbeiter ID: "))
                supplier_id = int(input("Lieferanten ID: "))

                more_products = "y"
                weight_products = []
                count_products = []
                while more_products != "n":
                    print()
                    product_id = int(input("Produkt ID: "))
                    product = self.db.select_product_with_id(product_id)
                    if product["TYP"] == "gewichtsbasiert":
                        product_weight = float(input("Gewicht Produkt: "))
                        weight_products.append(product)
                        product["PURCHASED_WEIGHT"] = product_weight
                    elif product["TYP"] == "stueckbasiert":
                        product_count = float(input("Anzahl Produkt: "))
                        for x in range(product_count):
                            count_products.append(product)
                    else:
                        print_error()
                        return
                    more_products = input("mehr Produkte einfügen (ja-ENTER: nein-n)")

                added_buying_id = self.db.insert_einkauf_row(buying_date=buying_date,
                                                             worker_id=worker_id,
                                                             supplier_id=supplier_id)
                self.db.calculate_and_insert_count_products_einkauf(added_buying_id, count_products)
                self.db.calculate_and_insert_weight_products_einkauf(added_buying_id, weight_products)
            elif selected_point == 3:
                print()
                print("-----Kunden-----")
                df_customers = pd.DataFrame(self.db.select_all_customers())
                print(df_customers)
            elif selected_point == 4:
                print()
                print("-----Mitarbeiter-----")
                df_workers = pd.DataFrame(self.db.select_all_workers())
                print(df_workers)
            elif selected_point == 5:
                print()
                print("-----Mitarbeiter Funktionen-----")
                df_workers = pd.DataFrame(self.db.select_all_worker_functions())
                print(df_workers)
            elif selected_point == 6:
                print()
                print("-----Lieferscheine-----")
                df_workers = pd.DataFrame(self.db.select_all_delivery_notes())
                print(df_workers)
            elif selected_point == 7:
                print()
                print("-----Rechnungen-----")
                df_workers = pd.DataFrame(self.db.select_all_invoices())
                print(df_workers)
            elif selected_point == 8:
                print()
                print("-----Bons-----")
                df_workers = pd.DataFrame(self.db.select_all_bons())
                print(df_workers)
            else:
                print_error()
                return


terminal = Terminal()
# pd.options.display.max_columns = None
# pd.options.display.max_rows = None
terminal.start()
