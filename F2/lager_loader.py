from db_service import DB_F2


class LagerLoader:

    def __init__(self):
        self.db = DB_F2()

    def create_lagereinheiten_into_database(self):
        products = self.db.select_products()
        current_shelf_number = 53
        for storage_unit_type in ["Verkaufslaeche", "Lagerflaeche"]:
            produce = ProduceShelf(current_shelf_number + 1, self.db)
            gebaek = BakedGoodShelf(current_shelf_number + 2, self.db)
            getraenke = DrinkShelf(current_shelf_number + 3, self.db)
            gefrier = FreezerShelf(current_shelf_number + 4, self.db)
            kuehl = CooledGoodsShelf(current_shelf_number + 5, self.db)
            rest = NormalShelf(current_shelf_number + 6, self.db)
            current_shelf_number = current_shelf_number + 6
            for product in products:
                product_category_id = self.db.select_categoryid_from_productid(product[0])
                if product_category_id in [282, 283, 290, 298, 321, 323]:
                    if not gebaek.space_available():
                        current_shelf_number = current_shelf_number.__add__(1)
                        gebaek = BakedGoodShelf(current_shelf_number, self.db)
                    gebaek.use_space(product[0], storage_unit_type)
                elif product_category_id in [267, 272, 275, 276, 317, 269, 280, 327, 328, 329, 330, 331, 332]:
                    if not kuehl.space_available():
                        current_shelf_number = current_shelf_number.__add__(1)
                        kuehl = CooledGoodsShelf(current_shelf_number, self.db)
                    kuehl.use_space(product[0], storage_unit_type)
                elif product_category_id in [270, 273, 312, 313, 314, 320, 334, 337]:
                    if not gefrier.space_available():
                        current_shelf_number = current_shelf_number.__add__(1)
                        gefrier = FreezerShelf(current_shelf_number, self.db)
                    gefrier.use_space(product[0], storage_unit_type)
                elif product_category_id in [279, 286, 295, 296, 303]:
                    if not getraenke.space_available():
                        current_shelf_number = current_shelf_number.__add__(1)
                        getraenke = DrinkShelf(current_shelf_number, self.db)
                    getraenke.use_space(product[0], storage_unit_type)
                elif product_category_id in [274, 310, 333]:
                    if not produce.space_available():
                        current_shelf_number = current_shelf_number.__add__(1)
                        produce = ProduceShelf(current_shelf_number, self.db)
                    produce.use_space(product[0], storage_unit_type)
                else:
                    if not rest.space_available():
                        current_shelf_number = current_shelf_number.__add__(1)
                        rest = NormalShelf(current_shelf_number, self.db)
                    rest.use_space(product[0], storage_unit_type)

    def run(self):
        self.create_lagereinheiten_into_database()


class Shelf:
    ids = []
    standard_width = 12
    standard_depth = 20
    standard_height = 16

    def __init__(self, number: int, line: int, column: int, db: DB_F2):
        self.db = db
        self.number = number
        self.lines = line
        self.columns = column
        self.free_units = self.lines * self.columns
        self.current_line = 0
        self.current_column = 0

    def space_available(self) -> bool:
        return self.free_units > 0

    def is_end_of_row(self) -> bool:
        return self.current_column == self.columns

    def is_category_appropriate(self, category_id: int):
        return category_id in self.ids

    def jump_to_next_line(self):
        self.current_column = 0
        self.current_line = self.current_line + 1

    def next_storage_unit(self):
        self.current_column = self.current_column + 1
        self.free_units = self.free_units.__sub__(1)

    def insert_data_into_database(self, product_id: int, unit_type: str):
        storage_unit_id = self.db.insert_lagereinheit(nummer=self.number,
                                                      zeile=self.current_line,
                                                      spalte=self.current_column,
                                                      breite=self.standard_width,
                                                      tiefe=self.standard_depth,
                                                      hoehe=self.standard_height,
                                                      typ=unit_type)
        self.db.insert_product_to_lagereinheit(product_id, storage_unit_id)

    def use_space(self, product_id: int, unit_type: str):
        if self.space_available():
            print("Insert successfull")
            # self.insert_data_into_database(product_id, unit_type)
            if self.is_end_of_row and self.space_available():
                self.jump_to_next_line()
            self.insert_data_into_database(product_id=product_id,
                                           unit_type=unit_type)
            self.next_storage_unit()
        else:
            raise RuntimeError("No space is available in storage unit " + str(self.number))


class ProduceShelf(Shelf):
    ids = [274, 310, 333]

    def __init__(self, shelf_number: id, db: DB_F2):
        super().__init__(shelf_number, 1, 5, db)

    @staticmethod
    def create_new(shelf_number: int, db: DB_F2):
        return ProduceShelf(shelf_number, db)


class DrinkShelf(Shelf):
    ids = [279, 286, 295, 296, 303]

    def __init__(self, shelf_number: id, db: DB_F2):
        super().__init__(shelf_number, 6, 10, db)

    @staticmethod
    def create_new(shelf_number: int, db: DB_F2):
        return DrinkShelf(shelf_number, db)


class FreezerShelf(Shelf):
    ids = [270, 273, 312, 313, 314, 320, 334, 337]

    def __init__(self, shelf_number: id, db: DB_F2):
        super().__init__(shelf_number, 6, 15, db)

    @staticmethod
    def create_new(shelf_number: int, db: DB_F2):
        return FreezerShelf(shelf_number, db)


class BakedGoodShelf(Shelf):
    ids = [282, 283, 290, 298, 321, 323]

    def __init__(self, shelf_number: id, db: DB_F2):
        super().__init__(shelf_number, 4, 5, db)

    @staticmethod
    def create_new(shelf_number: int, db: DB_F2):
        return BakedGoodShelf(shelf_number, db)


class CooledGoodsShelf(Shelf):
    ids = [267, 272, 275, 276, 317, 269, 280, 327, 328, 329, 330, 331, 332]

    def __init__(self, shelf_number: id, db: DB_F2):
        super().__init__(shelf_number, 6, 15, db)

    @staticmethod
    def create_new(shelf_number: int, db: DB_F2):
        return CooledGoodsShelf(shelf_number, db)


class NormalShelf(Shelf):
    def __init__(self, shelf_number: id, db: DB_F2):
        super().__init__(shelf_number, 15, 20, db)

    @staticmethod
    def create_new(shelf_number: int, db: DB_F2):
        return NormalShelf(shelf_number, db)
