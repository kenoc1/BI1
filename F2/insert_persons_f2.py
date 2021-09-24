from random import random, randint

import cx_Oracle

import config
import util
from db_service import DB_F2

con = cx_Oracle.connect(user=config.DB_CON_USER_F2, password=config.DB_CON_PW_F2, dsn=config.DB_CON_DSN_F2,
                        encoding="UTF-8")
print("Database version:", con.version)


def insert_funktionen():
    functions = config.FUNCTIONS_F2
    try:
        with con.cursor() as cursor:
            sql = ('Insert into FUNKTION(BEZEICHNUNG)'
                   'values(:bezeichnung)')
            for function in functions:
                cursor.execute(sql, [function])
                con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_mitarbeiter_function():
    try:
        with con.cursor() as cursor:
            cursor.execute(f"""select Mitarbeiter_Id, GEHALT from MITARBEITER""")
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(row)
                    worker_id = row[0]
                    salary = row[1]
                    if salary > 12000:
                        function_id = db.get_id_from_function("Filialleiter*in")
                    elif salary > 6000:
                        function_id = db.get_id_from_function("Abteilungsleiter*in")
                    elif salary > 4000:
                        mittel = [db.get_id_from_function("Einkäufer"), db.get_id_from_function("Raumpfleger*in"),
                                  db.get_id_from_function("Consultant")]
                        function_id = mittel[randint(0, len(mittel) - 1)]
                    else:
                        gering = [db.get_id_from_function("Azubi"), db.get_id_from_function("Oberkassierer*in"),
                                  db.get_id_from_function("Kassierer*in"), db.get_id_from_function("Praktikant*in"),
                                  db.get_id_from_function("Dualer Student*in"), db.get_id_from_function("Lagerist")]
                        function_id = gering[randint(0, len(gering) - 1)]
                    try:
                        with con.cursor() as cursor2:
                            sql = ('Insert into ZUWEISUNG_MITARBEITER_FUNKTION(FUNKTIONS_ID, MITARBEITER_ID)'
                                   'values(:function_id,:worker_id)')
                            cursor2.execute(sql, [function_id, worker_id])
                            con.commit()
                    except cx_Oracle.Error as error:
                        print('Error occurred in cursor 2:')
                        print(error)
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def _insert_mitarbeiter_salary():
    try:
        with con.cursor() as cursor:
            cursor.execute(f"""select Mitarbeiter_Id from MITARBEITER""")
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    worker_id = row[0]
                    try:
                        with con.cursor() as cursor2:
                            sql = ('UPDATE MITARBEITER SET "Gehalt" = salary WHERE MITARBEITER_ID = worker_id'
                                   'values(:salary,:worker_id)')
                            cursor2.execute(sql, [util.random_salary(), worker_id])
                    except cx_Oracle.Error as error:
                        print('Error occurred in cursor 2:')
                        print(error)
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_mitarbeiter(range_number):
    print("Inserting Mitarbeiter rows....")
    address_id_min = db.get_address_id_min()
    address_id_max = db.get_address_id_max()
    for n in range(range_number):
        db.insert_mitarbeiter_row(util.generate_firstname(), util.generate_lastname(), util.random_commission_rate(),
                                  util.random_salary(), _random_address_id(address_id_min, address_id_max))


def insert_kunden(range_number):
    print("Inserting Kunden rows....")
    address_id_min = db.get_address_id_min()
    address_id_max = db.get_address_id_max()
    for n in range(range_number):
        db.insert_kunde_row(util.generate_firstname(), util.generate_lastname(),
                            util.random_date_for_priceloader('1/1/1970 1:30 PM', '1/1/2005 4:50 AM', random()),
                            _random_address_id(address_id_min, address_id_max),
                            _random_address_id(address_id_min, address_id_max))


def _random_address_id(start_address_id, end_address_id):
    present = False
    while not present:
        address_id = randint(start_address_id, end_address_id)
        present = db.address_present(address_id)
    return address_id


def _testing():
    print("testing....")
    # _insert_kunden(300)
    # _insert_mitarbeiter(10)

    # print(_address_present(32717))
    # address_id_min = _get_address_id_min()
    # address_id_max = _get_address_id_max()
    # print(f"Address Min: {address_id_min}")
    # print(f"Address Max: {address_id_max}")
    # select_table("ORTSKENNZAHL")
    # print(f"Commission Rate Random: {_random_commission_rate()}")
    # print(f"Address Random: {_random_address_id(address_id_min, address_id_max)}")
    # print(f"Date Random: {_random_date('1/1/1970 1:30 PM', '1/1/2005 4:50 AM', random())}")

    # _insert_kunde_row(_generate_firstname(), _generate_lastname(),
    #                   _random_date('1/1/1970 1:30 PM', '1/1/2005 4:50 AM', random()),
    #                   _random_address_id(address_id_min, address_id_max),
    #                   _random_address_id(address_id_min, address_id_max))
    # _insert_mitarbeiter_row(_generate_firstname(), _generate_lastname(), _random_commission_rate(),
    #                         _random_address_id(address_id_min, address_id_max))
    # insert_mitarbeiter_function()
    # _insert_mitarbeiter_salary()
    # insert_funktionen()
    # insert_mitarbeiter(800)
    # insert_mitarbeiter_function()
    # insert_kunden(3000)


print("Launching...")
db = DB_F2()
_testing()
