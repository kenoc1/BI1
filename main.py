import time
from random import random, seed, gauss, randint

import config
import cx_Oracle
import names

con = cx_Oracle.connect(user=config.DB_CON_USER, password=config.DB_CON_PW, dsn=config.DB_CON_DSN, encoding="UTF-8")
print("Database version:", con.version)


def select_table(table_name):
    try:
        with con.cursor() as cursor:
            # cursor.execute("""select * from ORTSKENNZAHL""")
            cursor.execute(f"select * from {table_name}")
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(row)
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_mitarbeiter(billing_date, amount, customer_id, note):
    try:
        sql = ('insert into MITARBEITER(VORNAME, NACHNAME, PROVISONSSATZ, ADRESS_ID) '
               'values(:vname,:lname,:provisionssatz,:adresse_id)')
        # create a cursor
        with con.cursor() as cursor:
            # execute the insert statement
            cursor.execute(sql, [billing_date, amount, customer_id, note])
            # commit work
            con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_kunde(billing_date, amount, customer_id, note):
    try:
        sql = ('insert into KUNDE(VORNAME, NACHNAME, GEBURTSDATUM, RECHNUNGS_ADRESSE_ID, LIEFER_ADRESSE_ID) '
               'values(:vname,:lname,:birthdate,:rechnungs_adresse_id,:liefer_adresse_id)')
        # create a cursor
        with con.cursor() as cursor:

            # execute the insert statement
            cursor.execute(sql, [billing_date, amount, customer_id, note])
            # commit work
            con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def __generate_firstname():
    return names.get_first_name()


def __generate_lastname():
    return names.get_last_name()


def __random_adress_id():
    return randint(0, 100)


def _str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def _random_date(start, end, prop):
    return _str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)


if __name__ == '__main__':
    print("Launching...")
    seed(1)
    select_table("ORTSKENNZAHL")
    print(__random_adress_id())
    print(_random_date("1/1/1970 1:30 PM", "1/1/2005 4:50 AM", random()))
