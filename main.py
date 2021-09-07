import time
from random import random, seed, gauss, randint, uniform
import config
import cx_Oracle
import names
from util import Util
from db_service import DB

con = cx_Oracle.connect(user=config.DB_CON_USER, password=config.DB_CON_PW, dsn=config.DB_CON_DSN, encoding="UTF-8")
print("Database version:", con.version)


def select_table(table_name):
    try:
        with con.cursor() as cursor:
            # cursor.execute("""select * from ORTSKENNZAHL""")
            cursor.execute(f"""select * from {table_name}""")
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(row)
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


# 12,Kassierer*in
# 13,Einkäufer
# 14,Abteilungsleiter*in
# 15,Oberkassierer*in
# 16,Consultant
# 17,Raumpfleger*in
# 18,Praktikant*in
# 19,Dualer Student*in
# 20,Azubi
# 21,Lagerist
# 22,Filialleiter*in

def insert_funktionen():
    functions = ["Einkäufer",
                 "Oberkassierer*in",
                 "Consultant",
                 "Raumpfleger*in",
                 "Kassierer*in",
                 "Praktikant*in",
                 "Dualer Student*in",
                 "Azubi",
                 "Lagerist",
                 "Abteilungsleiter*in",
                 "Filialleiter*in"]
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
                        function_id = _get_id_from_function("Filialleiter*in")
                    elif salary > 6000:
                        function_id = _get_id_from_function("Abteilungsleiter*in")
                    elif salary > 4000:
                        mittel = [_get_id_from_function("Einkäufer"), _get_id_from_function("Raumpfleger*in"),
                                  _get_id_from_function("Consultant")]
                        function_id = mittel[randint(0, len(mittel) - 1)]
                    else:
                        gering = [_get_id_from_function("Azubi"), _get_id_from_function("Oberkassierer*in"),
                                  _get_id_from_function("Kassierer*in"), _get_id_from_function("Praktikant*in"),
                                  _get_id_from_function("Dualer Student*in"), _get_id_from_function("Lagerist")]
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
                            cursor2.execute(sql, [_random_salary(), worker_id])
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
        _insert_mitarbeiter_row(_generate_firstname(), _generate_lastname(), _random_commission_rate(),
                                _random_salary(), _random_address_id(address_id_min, address_id_max))


def insert_kunden(range_number):
    print("Inserting Kunden rows....")
    address_id_min = db.get_address_id_min()
    address_id_max = db.get_address_id_max()
    for n in range(range_number):
        _insert_kunde_row(_generate_firstname(), _generate_lastname(),
                          _random_date('1/1/1970 1:30 PM', '1/1/2005 4:50 AM', random()),
                          _random_address_id(address_id_min, address_id_max),
                          _random_address_id(address_id_min, address_id_max))


def _insert_mitarbeiter_row(first_name, last_name, commission_rate, salary, address_id):
    sql = ('insert into MITARBEITER(VORNAME, NACHNAME, PROVISIONSSATZ, GEHALT, ADRESS_ID)'
           'values(:first_name,:last_name,:commission_rate,:salary,:address_id)')
    print("Mitarbeiter: ", [first_name, last_name, commission_rate, salary, address_id])
    try:
        # create a cursor
        with con.cursor() as cursor:
            # execute the insert statement
            cursor.execute(sql, [first_name, last_name, commission_rate, address_id])
            # commit work
            con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def _insert_kunde_row(first_name, last_name, birthdate, billing_address, shipping_address):
    sql = ("insert into KUNDE(VORNAME, NACHNAME, GEBURTSDATUM, RECHNUNGS_ADRESSE_ID, LIEFER_ADRESSE_ID)"
           "values(:first_name,:last_name, to_date(:birthdate,'MM/DD/YYYY HH:MI AM'),:billing_address,:shipping_address)")
    print("Kunden: ", [first_name, last_name, birthdate, billing_address, shipping_address])
    try:
        # create a cursor
        with con.cursor() as cursor:
            # execute the insert statement
            cursor.execute(sql, [first_name, last_name, birthdate, billing_address, shipping_address])
            # commit work
            con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def _generate_firstname() -> str:
    return names.get_first_name()


def _generate_lastname() -> str:
    return names.get_last_name()


def _random_salary() -> float:
    return uniform(300, 8000)


def _random_commission_rate() -> float:
    return round(uniform(0, 5), 2)


def _random_address_id(start_address_id, end_address_id):
    present = False
    while not present:
        address_id = randint(start_address_id, end_address_id)
        present = _address_present(address_id)
    return address_id


def _address_present(address_id):
    with con.cursor() as cursor:
        cursor.execute("""select * from ADRESSE WHERE ADRESS_ID = :address_id""", address_id=address_id)
        row = cursor.fetchone()
        if row:
            return True
        else:
            False


def _get_id_from_function(function_description):
    try:
        with con.cursor() as cursor:
            cursor.execute("""select * from FUNKTION WHERE BEZEICHNUNG = :function_description""",
                           function_description=function_description)
            row = cursor.fetchone()
        if row:
            return row[0]
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def _str_time_prop(start, end, time_format, prop) -> str:
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def _random_date(start, end, prop) -> str:
    return _str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)


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
    insert_mitarbeiter_function()
    # _insert_mitarbeiter_salary()
    # insert_funktionen()


if __name__ == '__main__':
    print("Launching...")
    util = Util()
    db = DB()
    _testing()
