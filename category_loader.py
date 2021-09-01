import csv
import config
import cx_Oracle

con = cx_Oracle.connect(user=config.DB_CON_USER, password=config.DB_CON_PW, dsn=config.DB_CON_DSN, encoding="UTF-8")
print("Database version:", con.version)


def load_food_subcategories():
    with open('produkt_kategorien_uebersetzt.csv', newline='') as csvfile:
        categories = csv.reader(csvfile, delimiter=';')
        subcategories = []
        if categories:
            for category in categories:
                subcategories.append((category[0], category[1]))
    return subcategories


def insert_subcategory(bezeichnung, alters):
    try:
        sql = (
            'insert into PRODUKTKATEGORIE(BEZEICHNUNG, ALTERFREIGABE) '  # PRODUKT_HOEHE, PRODUKT_TIEFE, PRODUKT_BREITE
            'values(:bezeichnung, :alters)')
        # create a cursor
        with con.cursor() as cursor:
            # execute the insert statement
            cursor.execute(sql, [bezeichnung, alters])
            # commit work
            con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_product_subcategory(productid, catid):
    try:
        sql = (
            'insert into ZUWEISUNG_PRODUKT_PRODUKTKATEGORIE(PRODUKT_ID, PRODUKTKATEGORIE_ID) '  # PRODUKT_HOEHE, PRODUKT_TIEFE, PRODUKT_BREITE
            'values(:productid, :catid)')
        # create a cursor
        with con.cursor() as cursor:
            print("In Insert")
            # execute the insert statement
            cursor.execute(sql, [productid, catid])
            print("Execute")
            # commit work
            con.commit()
            print("Commited Insert")
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def get_product_id_by_name(productname: str):
    try:
        with con.cursor() as cursor:
            # cursor.execute("""select * from ORTSKENNZAHL""")
            cursor.execute(f"select PRODUKT_ID from PRODUKT WHERE BEZEICHNUNG = :name", name=productname)
            rows = cursor.fetchall()
            if rows:
                return rows[0][0]
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def load_subcategory_csv_into_database():
    with open('produkt_kategorien_uebersetzt.csv', newline='') as csvfile:
        categories = csv.reader(csvfile, delimiter=';')
        for category in categories:
            if category[4] in ['Food', 'Drink'] or category[2] in ['Electrical']:
                insert_subcategory(category[1], (16 if category[3] in ['Alcoholic Beverages'] else 0))


def get_subcategory_by_excelid(excelid: str) -> str:
    for cat in subcats[1:]:
        num = int(cat[0])
        excelnum = int(excelid)
        if num == excelnum:
            return cat[1]


def get_category_id_by_name(categoryname: str):
    try:
        with con.cursor() as cursor:
            # cursor.execute("""select * from ORTSKENNZAHL""")
            cursor.execute(f"select PRODUKTKATEGORIE_ID from PRODUKTKATEGORIE WHERE BEZEICHNUNG = :name",
                           name=categoryname)
            rows = cursor.fetchall()
            if rows:
                return rows[0][0]
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def load_category_to_product_into_database():
    with open('produkte.csv', newline='') as csvfile:
        products = csv.reader(csvfile, delimiter=';')
        c = 0
        for product in products:
            if c > 0:
                product_id = get_product_id_by_name(product[3])
                category_name = get_subcategory_by_excelid(product[0])
                category_id = get_category_id_by_name(category_name)
                print(product_id)
                insert_product_subcategory(product_id, category_id)
                print(category_id)
            c = c + 1

def insert_oberkategorie_subcategory(catid, ocatid):
    try:
        sql = (
            'insert into ZUWEISUNG_KATEGORIE_OBERKATEGORIE(PRODUKTKATEGORIE_ID, PRODUKTOBERKATEGORIE_ID) '  # PRODUKT_HOEHE, PRODUKT_TIEFE, PRODUKT_BREITE
            'values(:catid, :ocatid)')
        # create a cursor
        with con.cursor() as cursor:
            print("In Insert")
            # execute the insert statement
            cursor.execute(sql, [catid, ocatid])
            print("Execute")
            # commit work
            con.commit()
            print("Commited Insert")
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)

def load_category_to_category_into_database():
    with open('produkt_kategorien_uebersetzt.csv', newline='') as csvfile:
        categories = csv.reader(csvfile, delimiter=';')
        for category in categories:
            category_id = get_category_id_by_name(category[1])
            if category[4] in ["Food", "Drink"]:
                insert_oberkategorie_subcategory(category_id, 1)
            else:
                insert_oberkategorie_subcategory(category_id, 2)

subcats = load_food_subcategories()
# load_category_to_category_into_database()
load_category_to_product_into_database()