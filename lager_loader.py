from db_interact import DB

db = DB()


# db = None


def create_lagereinheiten_into_database():
    products = db.select_products()
    current_shelf_number = 53
    for storage_unit_type in ["Verkaufslaeche", "Lagerflaeche"]:
        produce = ProduceShelf(current_shelf_number + 1)
        gebaek = BakedGoodShelf(current_shelf_number + 2)
        getraenke = DrinkShelf(current_shelf_number + 3)
        gefrier = FreezerShelf(current_shelf_number + 4)
        kuehl = CooledGoodsShelf(current_shelf_number + 5)
        rest = NormalShelf(current_shelf_number + 6)
        current_shelf_number = current_shelf_number + 6
        for product in products:
            product_category_id = db.select_categoryid_from_productid(product[0])
            if product_category_id in [282, 283, 290, 298, 321, 323]:
                if not gebaek.space_available():
                    current_shelf_number = current_shelf_number.__add__(1)
                    gebaek = BakedGoodShelf(current_shelf_number)
                gebaek.use_space(product[0], storage_unit_type)
            elif product_category_id in [267, 272, 275, 276, 317, 269, 280, 327, 328, 329, 330, 331, 332]:
                if not kuehl.space_available():
                    current_shelf_number = current_shelf_number.__add__(1)
                    kuehl = CooledGoodsShelf(current_shelf_number)
                kuehl.use_space(product[0], storage_unit_type)
            elif product_category_id in [270, 273, 312, 313, 314, 320, 334, 337]:
                if not gefrier.space_available():
                    current_shelf_number = current_shelf_number.__add__(1)
                    gefrier = FreezerShelf(current_shelf_number)
                gefrier.use_space(product[0], storage_unit_type)
            elif product_category_id in [279, 286, 295, 296, 303]:
                if not getraenke.space_available():
                    current_shelf_number = current_shelf_number.__add__(1)
                    getraenke = DrinkShelf(current_shelf_number)
                getraenke.use_space(product[0], storage_unit_type)
            elif product_category_id in [274, 310, 333]:
                if not produce.space_available():
                    current_shelf_number = current_shelf_number.__add__(1)
                    produce = ProduceShelf(current_shelf_number)
                produce.use_space(product[0], storage_unit_type)
            else:
                if not rest.space_available():
                    current_shelf_number = current_shelf_number.__add__(1)
                    rest = NormalShelf(current_shelf_number)
                rest.use_space(product[0], storage_unit_type)


class ShelfLoader:
    def __init__(self, current_shelf_number: int):
        self.products = db.select_products()
        self.shelf_manager = ShelfManager(current_shelf_number)

    def load_product_to_shelf_assignment_into_database(self):
        if self.products:
            for product in self.products:
                product_category_id = db.select_categoryid_from_productid(product[0])
                for storage_unit_type in ["Verkaufslaeche", "Lagerflaeche"]:
                    self.shelf_manager.add_to_shelf(category_id=product_category_id,
                                                    product_id=product[0],
                                                    storage_unit_type=storage_unit_type)
        else:
            raise RuntimeError("No products could be fetched from the database")


class ShelfManager:
    backed_ids = [282, 283, 290, 298, 321, 323]
    cooled_goods = [267, 272, 275, 276, 317, 269, 280, 327, 328, 329, 330, 331, 332]
    frozen_ids = [270, 273, 312, 313, 314, 320, 334, 337]
    drink_ids = [279, 286, 295, 296, 303]
    produce_ids = [274, 310, 333]

    def __init__(self, starting_shelf_number: int):
        self.produce = ProduceShelf(starting_shelf_number)
        # self.backed_goods = BakedGoodShelf(starting_shelf_number + 1)
        # self.drinks = DrinkShelf(starting_shelf_number + 2)
        # self.frozen_goods = FreezerShelf(starting_shelf_number + 3)
        # self.cooled_goods = CooledGoodsShelf(starting_shelf_number + 4)
        # self.rest = NormalShelf(starting_shelf_number + 5)
        self.current_shelf_number = starting_shelf_number + 5
        self.all_shelves = [ProduceShelf(starting_shelf_number),
                            BakedGoodShelf(starting_shelf_number + 1),
                            DrinkShelf(starting_shelf_number + 2),
                            FreezerShelf(starting_shelf_number + 3),
                            CooledGoodsShelf(starting_shelf_number + 4),
                            NormalShelf(starting_shelf_number + 5)
                            ]

    def add_to_shelf(self, category_id: int, product_id: int, storage_unit_type: str):
        print(self.all_shelves)
        if category_id and product_id and storage_unit_type:
            for shelf, index in self.all_shelves:
                print(shelf)
                if not shelf.space_available():
                    self.current_shelf_number = self.current_shelf_number.__add__(1)
                    shelf = shelf.create_new()
                if shelf.is_category_appropriate(category_id=category_id):
                    shelf.use_space(product_id=product_id,
                                    unit_type=storage_unit_type)


class Shelf:
    ids = []
    standard_width = 12
    standard_depth = 20
    standard_height = 16

    def __init__(self, number: int, line: int, column: int):
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
        storage_unit_id = db.insert_lagereinheit(nummer=self.number,
                                                 zeile=self.current_line,
                                                 spalte=self.current_column,
                                                 breite=self.standard_width,
                                                 tiefe=self.standard_depth,
                                                 hoehe=self.standard_height,
                                                 typ=unit_type)
        db.insert_product_to_lagereinheit(product_id, storage_unit_id)

    def use_space(self, product_id: int, unit_type: str):
        if self.space_available():
            print("Insert successfull")
            # self.insert_data_into_database(product_id, unit_type)
            if self.is_end_of_row and self.space_available():
                self.jump_to_next_line()
            self.next_storage_unit()
        else:
            raise RuntimeError("No space is available in storage unit " + str(self.number))


class ProduceShelf(Shelf):
    ids = [274, 310, 333]

    def __init__(self, shelf_number: id):
        super().__init__(shelf_number, 1, 5)

    @staticmethod
    def create_new(shelf_number: int):
        return ProduceShelf(shelf_number)


class DrinkShelf(Shelf):
    ids = [279, 286, 295, 296, 303]

    def __init__(self, shelf_number: id):
        super().__init__(shelf_number, 6, 10)

    @staticmethod
    def create_new(shelf_number: int):
        return DrinkShelf(shelf_number)


class FreezerShelf(Shelf):
    ids = [270, 273, 312, 313, 314, 320, 334, 337]

    def __init__(self, shelf_number: id):
        super().__init__(shelf_number, 6, 15)

    @staticmethod
    def create_new(shelf_number: int):
        return FreezerShelf(shelf_number)


class BakedGoodShelf(Shelf):
    ids = [282, 283, 290, 298, 321, 323]

    def __init__(self, shelf_number: id):
        super().__init__(shelf_number, 4, 5)

    @staticmethod
    def create_new(shelf_number: int):
        return BakedGoodShelf(shelf_number)


class CooledGoodsShelf(Shelf):
    ids = [267, 272, 275, 276, 317, 269, 280, 327, 328, 329, 330, 331, 332]

    def __init__(self, shelf_number: id):
        super().__init__(shelf_number, 6, 15)

    @staticmethod
    def create_new(shelf_number: int):
        return CooledGoodsShelf(shelf_number)


class NormalShelf(Shelf):
    def __init__(self, shelf_number: id):
        super().__init__(shelf_number, 15, 20)

    @staticmethod
    def create_new(shelf_number: int):
        return NormalShelf(shelf_number)


# sl = ShelfLoader(1)
# sl.load_product_to_shelf_assignment_into_database()

create_lagereinheiten_into_database()
