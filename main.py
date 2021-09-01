import config
import cx_Oracle

db_connection_dsn = cx_Oracle.makedsn("134.106.56.54", 1521, service_name="fastdbwin")

con = cx_Oracle.connect(user=config.DB_CON_USER, password=config.DB_CON_PW, dsn=db_connection_dsn, encoding="UTF-8")
print("Database version:", con.version)


def select_ortskennzahl():
    try:
        with con.cursor() as cursor:
            cursor.execute("""select * from ORTSKENNZAHL""")
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(row)
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_row(billing_date, amount, customer_id, note):
    try:
        sql = ('insert into billing_headers(billing_date, amount, customer_id, note) '
               'values(:billing_date,:amount,:customer_id,:note)')
        # create a cursor
        with con.cursor() as cursor:
            # execute the insert statement
            cursor.execute(sql, [billing_date, amount, customer_id, note])
            # commit work
            con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


if __name__ == '__main__':
    select_ortskennzahl()
