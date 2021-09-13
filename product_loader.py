import csv
import config
import cx_Oracle

con = cx_Oracle.connect(user=config.DB_CON_USER, password=config.DB_CON_PW, dsn=config.DB_CON_DSN, encoding="UTF-8")
print("Database version:", con.version)


def load_food_categories():
    food_categories = []
    with open('produkt_kategorien.csv', newline='') as csvfile:
        categories = csv.reader(csvfile, delimiter=';')
        if categories:
            for category in categories:
                if category[4] in ["Food", "Drink"]:
                    food_categories.append(category[0])
    return food_categories


def select_brand_id(brand_name: str):
    try:
        with con.cursor() as cursor:
            cursor.execute(f"select MARKE_ID from MARKE WHERE BEZEICHNUNG = :name", name=brand_name)
            rows = cursor.fetchall()
            return rows[0][0]
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_product(nettogewicht: float, umsatzsteuer: float, bezeichnung: str, sku: int, typ: str, marke: str,
                   bruttogewicht: float):
    try:
        sql = (
            'insert into PRODUKT(NETTOGEWICHT, UMSATZSTEUERSATZ, BEZEICHNUNG, SKU, TYP, MARKE_ID, BRUTTOGEWICHT) '  # PRODUKT_HOEHE, PRODUKT_TIEFE, PRODUKT_BREITE
            'values(:nettogewicht,:umsatzsteuer,:bezeichnung,:sku, :typ, :marke, :bruttogewicht)')
        # create a cursor
        with con.cursor() as cursor:
            # execute the insert statement
            cursor.execute(sql, [nettogewicht, umsatzsteuer, bezeichnung, sku, typ, marke, bruttogewicht])
            # commit work
            con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def oz_to_ibs(oz: float) -> float:
    return ((oz * 28.35) / 1000) * 2


def weight_to_correct_float(value: str) -> float:
    a = value.replace(",", ".")
    b = float(a)
    c = round(b, 4)
    return c


def load_product_csv_into_database():
    with open('produkte.csv', newline='') as csvfile:
        products = csv.reader(csvfile, delimiter=';')
        for product in products:
            if product[0] in food_categories:
                print(oz_to_ibs(weight_to_correct_float(product[7])), oz_to_ibs(weight_to_correct_float(product[6])))
                insert_product(oz_to_ibs(weight_to_correct_float(product[7])), 7.00, product[3], int(product[4]),
                               ('gewichtsbasiert' if int(product[0]) in weight_based_categories else 'stueckbasiert'),
                               select_brand_id(product[2]), oz_to_ibs(weight_to_correct_float(product[6])))


weight_based_categories = [24, 13, 61, 77, 99]
food_categories = load_food_categories()
# insert_product(14.1200, 19.00, 'Testprodukt', 1, 'gewichtsbasiert', 10, 1, 1, 1, 21.00)

load_product_csv_into_database()
