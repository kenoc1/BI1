import csv

from db_service import DB_F2


class CategoryExtractor:

    def __init__(self):
        self.db = DB_F2()
        self._translated_categories = []

    @staticmethod
    def _make_csv_line_into_list(line: list) -> list[str]:
        return ''.join(line).split(";")

    def _load_oberkategorien_from_database(self) -> None:
        self._oberkategorien: list = self.db.select_all_produktoberkategorien()

    def _load_subaktegorien_from_database(self) -> None:
        self._subkategorien: list = self.db.select_all_produktkategorien()

    def load_data(self) -> None:
        self._load_oberkategorien_from_database()
        self._load_subaktegorien_from_database()
        self._load_category_csv()

    def _get_category_csv_as_dict(self, path: str):
        category_dicts: list[dict] = []
        with open(path, mode='r') as category_file:
            reader = csv.reader(category_file)
            first_line: list = [header for header in self._make_csv_line_into_list(next(reader))]
            for row in reader:
                row_as_list: list[str] = self._make_csv_line_into_list(row)
                category_dicts.append(
                    {first_line[row_as_list.index(elem)]: elem for elem in row_as_list})
        return category_dicts

    def _load_category_csv(self) -> None:
        self._translated_categories = self._get_category_csv_as_dict(
            '../../csv-files/produkt_kategorien_uebersetzt.csv')
        self._original_categories = self._get_category_csv_as_dict('../../csv-files/produkt_kategorien.csv')

    @property
    def oberkategorien(self) -> list[dict]:
        return self._oberkategorien

    @property
    def subkategorien(self) -> list[dict]:
        return self._subkategorien

    @property
    def translated_category_csv(self) -> list[dict]:
        return self._translated_categories

    @property
    def original_category_csv(self) -> list[dict]:
        return self._original_categories


cl = CategoryExtractor()
cl.load_data()
# [print(cat) for cat in cl.oberkategorien]
[print(c) for c in cl.translated_category_csv]
