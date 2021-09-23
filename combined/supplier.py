from db_service import DB_F2, DB_MASTER


class Supplier:
    def __init__(self):
        self.db_f2 = DB_F2()
        self.db_master = DB_MASTER()


Supplier()


