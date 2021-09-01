import time
from random import random, seed, gauss, randint, uniform
import config
import cx_Oracle
import names

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


def _insert_mitarbeiter(range_number):
    print("Inserting Mitarbeiter rows....")
    address_id_min = _get_address_id_min()
    address_id_max = _get_address_id_max()
    for n in range(range_number):
        _insert_mitarbeiter_row(_generate_firstname(), _generate_lastname(), _random_commission_rate(),
                                _random_address_id(address_id_min, address_id_max))


def _insert_kunden(range_number):
    print("Inserting Kunden rows....")
    address_id_min = _get_address_id_min()
    address_id_max = _get_address_id_max()
    for n in range(range_number):
        _insert_kunde_row(_generate_firstname(), _generate_lastname(),
                          _random_date('1/1/1970 1:30 PM', '1/1/2005 4:50 AM', random()),
                          _random_address_id(address_id_min, address_id_max),
                          _random_address_id(address_id_min, address_id_max))


def _insert_mitarbeiter_row(first_name, last_name, commission_rate, address_id):
    sql = ('insert into MITARBEITER(VORNAME, NACHNAME, PROVISIONSSATZ, ADRESS_ID)'
           'values(:first_name,:last_name,:commission_rate,:address_id)')
    print("Mitarbeiter: ", [first_name, last_name, commission_rate, address_id])
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


def _get_address_id_min() -> int:
    try:
        with con.cursor() as cursor:
            cursor.execute("""select MIN(ADRESS_ID) from ADRESSE""")
            row = cursor.fetchone()
            if row:
                return row[0]
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def _get_address_id_max() -> int:
    try:
        with con.cursor() as cursor:
            cursor.execute("""select MAX(ADRESS_ID) from ADRESSE""")
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


if __name__ == '__main__':
    print("Launching...")
    _testing()
