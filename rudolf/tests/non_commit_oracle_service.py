import cx_Oracle

from rudolf.oracle_service import CombDBService, F2DBService


class NonCommitCombDBService(CombDBService):

    def __init__(self):
        super().__init__()

    def _insert(self, sql_statement: str) -> None:
        with self.con.cursor() as cursor:
            cursor.execute(sql_statement)

    def _insert_and_return_id(self, sql_statement: str, id_name: str) -> int:
        sql = sql_statement + "returning {} into :python_var".format(id_name)
        with self.con.cursor() as cursor:
            newest_id_wrapper = cursor.var(cx_Oracle.STRING)
            cursor.execute(sql, [newest_id_wrapper])
            newest_id = newest_id_wrapper.getvalue()
            return int(newest_id[0])

    def select_max_id_from_table(self, id_name: str, table_name: str):
        sql = "SELECT MAX({}) FROM {}".format(id_name, table_name)
        with self.con.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchone()
            return rows[0]


class NonCommitF2DBService(F2DBService):

    def __init__(self):
        super().__init__()

    def _insert(self, sql_statement: str) -> None:
        with self.con.cursor() as cursor:
            cursor.execute(sql_statement)

    def _insert_and_return_id(self, sql_statement: str, id_name: str) -> int:
        sql = sql_statement + "returning {} into :python_var".format(id_name)
        with self.con.cursor() as cursor:
            newest_id_wrapper = cursor.var(cx_Oracle.STRING)
            cursor.execute(sql, [newest_id_wrapper])
            newest_id = newest_id_wrapper.getvalue()
            return int(newest_id[0])

    def select_max_id_from_table(self, id_name: str, table_name: str):
        sql = "SELECT MAX({}) FROM {}".format(id_name, table_name)
        with self.con.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchone()
            return rows[0]


if __name__ == "__main__":
    print(NonCommitCombDBService().select_max_id_from_table("LIEFERANT_ID", "LIEFERANT"))
