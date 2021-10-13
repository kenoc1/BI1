import csv
from pathlib import Path


class EANFixer:

    def __init__(self):
        self._translated_categories = []
        self._original_categories = []

    def _get_category_csv_as_dict(self, path: Path):
        category_dicts: list[dict] = []
        with path.open(mode='r', encoding="utf8") as category_file:
            reader = csv.reader(category_file)
            first_line: list = [header for header in self._make_csv_line_into_list(next(reader))]
            for row in reader:
                row_as_list: list[str] = self._make_csv_line_into_list(row)
                category_dicts.append(
                    {first_line[row_as_list.index(elem)]: elem for elem in row_as_list})
        return category_dicts

    def _load_category_csv_files(self) -> None:
        self._translated_categories = self._get_category_csv_as_dict(
            Path().cwd().parent.parent / 'data' / 'csv-files' / 'produkt_kategorien_uebersetzt.csv')
        self._original_categories = self._get_category_csv_as_dict(
            Path().cwd().parent.parent / 'data' / 'csv-files' / 'produkt_kategorien.csv')
