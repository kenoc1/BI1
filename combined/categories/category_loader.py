from db_service import DB_MASTER


class CategoryLoader:

    def __init__(self):
        self.db_master = DB_MASTER()

    def insert_subkategorien_data_source_allocation(self, cat_id: int, herkunft_id: int) -> None:
        self.db_master.insert_product_subcategory(cat_id, herkunft_id)

    def insert_oberkategorien_data_source_allocation(self):
        pass
