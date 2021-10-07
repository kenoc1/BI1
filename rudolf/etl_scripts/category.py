import csv
import sys
from pathlib import Path

import cx_Oracle

from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoExcelIdFoundForGermanCategoryName
from rudolf.sqlite_service import SQLiteService
from rudolf.util import compare_strings


class CategoryExtractor:

    def __init__(self):
        self.con_f2 = F2DBService()
        self.con_rudolf = SQLiteService()

    @staticmethod
    def _make_csv_line_into_list(line: list) -> list[str]:
        return ''.join(line).split(";")

    def _load_oberkategorien_from_database(self) -> None:
        self._oberkategorien: list = self.con_f2.select_all_produktoberkategorien()

    def _load_subkategorien_from_database(self) -> None:
        self._subkategorien: list = self.con_f2.select_all_produktkategorien()

    def load_data(self) -> None:
        self._load_oberkategorien_from_database()
        self._load_subkategorien_from_database()
        self._load_category_csv_files()

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

    def f2_db_oberkategorien(self) -> list[dict]:
        return self._oberkategorien

    @property
    def f2_db_subkategorien(self) -> list[dict]:
        return self._subkategorien

    @property
    def german_category_csv(self) -> list[dict]:
        return self._translated_categories

    @property
    def english_category_csv(self) -> list[dict]:
        return self._original_categories


class CategoryTransformer:

    def __init__(self):
        self.db_master = CombDBService()
        self.category_extractor = CategoryExtractor()
        self.category_extractor.load_data()
        self.category_translator = _SubcategoryTranslator(german_csv=self.category_extractor.german_category_csv,
                                                          english_csv=self.category_extractor.english_category_csv)
        self.category_loader = CategoryLoader()

    def _is_oberkategorie_mappable(self, english_oberkategorie_name: str) -> bool:
        pass

    def _map_oberkategorien(self):
        # DB:  {'PRODUKTOBERKATEGORIE_ID': 1, 'BEZEICHNUNG': 'Lebensmittel'}
        # CSV: {'product_class_id': '1', 'product_subcategory': 'Nüsse', 'product_category': 'Spezialität', 'product_department': 'Erzeugnis', 'product_family': 'Food'}
        pass

    def _query_subkategorie_by_name(self, english_subkategorie_name: str) -> list:
        try:
            return self.db_master.select_produktkategorie_where_bezeichnung(english_subkategorie_name)
        except cx_Oracle.Error as error:
            print('Database error occurred:')
            print(error)
            sys.exit("Datenbankverbindung erzeugt Fehler, Skript wird gestoppt!")

    def _is_subkategorie_mappable(self, english_subkategorie_name: str) -> bool:
        """
        Assesses whether a subcategory from Filiale2 can be unambiguously mapped to a subcategory from Onlineshop.
        :param english_subkategorie_name: The english name of a subcategory that is assessed.
        :return: True if there is exactly one possible mapping, else False
        """
        selected_entries = self.db_master.select_produktkategorie_where_bezeichnung(english_subkategorie_name)
        return True if len(selected_entries) == 1 else False

    def _exists_subkategorie(self, english_subkategorie_name: str) -> bool:
        selected_entries = self.db_master.select_produktkategorie_where_bezeichnung(english_subkategorie_name)
        return True if len(selected_entries) > 0 else False

    @staticmethod
    def _write_manual_check_to_file(content: dict):
        key_allocation_saver.write_to_csv(header=[],
                                          rows=[[content]],
                                          filepath=config.MANUAL_PRODUCT_SUB_CAT_CON_FILE_NAME)

    @staticmethod
    def _write_id_allocation_to_file(row: list):
        key_allocation_saver.write_to_csv(header=[],
                                          rows=[row],
                                          filepath=config.PRODUCT_SUB_CAT_CON_FILE_NAME)

    def _map_subkategorien(self):
        """
            F2 Beispiel-Dict: {'PRODUKTKATEGORIE_ID': 262, 'BEZEICHNUNG': 'Nüsse', 'ALTERFREIGABE': 0}
            CSV Beispiel-Dict: {'product_class_id': '1', 'product_subcategory': 'Nüsse', 'product_category': 'Spezialität', 'product_department': 'Erzeugnis', 'product_family': 'Food'}
            COMBINED Beispiel-Dict: {'PRODUKT_SUBKATEGORIE_ID': 1, 'PRODUKT_KATEGORIE_ID': 2, 'BEZEICHNUNG': 'Nuts'}
        """
        for subkategorie_entry in self.category_extractor.f2_db_subkategorien:
            try:
                translated_name: str = self.category_translator.translate(
                    german_subkategorie_name=subkategorie_entry.get("BEZEICHNUNG"))
                if not self._is_subkategorie_mappable(english_subkategorie_name=translated_name):
                    if not self._exists_subkategorie(english_subkategorie_name=translated_name):
                        new_id: int = self.db_master.insert_subcategory(subcat_name=translated_name)
                        self._write_id_allocation_to_file([new_id, subkategorie_entry.get("PRODUKTKATEGORIE_ID")])
                        self.category_loader.insert_subkategorien_data_source_allocation(new_id, config.SOURCE_F2)
                    else:
                        self._write_manual_check_to_file(subkategorie_entry)
                else:
                    existing_id = self._query_subkategorie_by_name(translated_name)[0].get("PRODUKT_SUBKATEGORIE_ID")
                    self._write_id_allocation_to_file([existing_id, subkategorie_entry.get("PRODUKTKATEGORIE_ID")])
                    self.category_loader.insert_subkategorien_data_source_allocation(existing_id, config.SOURCE_F2)
            except NoExcelIdFoundForGermanCategoryName:
                print("Not translation found for: {}".format(subkategorie_entry.__str__()))
                self._write_manual_check_to_file(subkategorie_entry)
            except cx_Oracle.Error as error:
                print('Database error occurred:')
                print(error)
                sys.exit("Datenbankverbindung erzeugt Fehler, Skript wird gestoppt!")

    def run(self):
        self._map_subkategorien()


class _SubcategoryTranslator:
    def __init__(self, german_csv: list[dict], english_csv: list[dict]):
        self.german_csv = german_csv
        self.english_csv = english_csv

    def _get_excel_id_by_translated_subcategory(self, german_subcategory: str) -> int:
        for german_category_csv_entry in self.german_csv:
            if compare_strings(german_subcategory, german_category_csv_entry.get("product_subcategory")):
                return german_category_csv_entry.get("product_class_id")

    def _get_english_name_by_excel_id(self, excel_id: int) -> str:
        for original_csv_category in self.english_csv:
            if excel_id == int(original_csv_category.get("product_class_id")):
                return original_csv_category.get("product_subcategory")

    def translate(self, german_subkategorie_name: str) -> str:
        excel_id = self._get_excel_id_by_translated_subcategory(german_subcategory=german_subkategorie_name)
        if excel_id is None:
            raise NoExcelIdFoundForGermanCategoryName(german_subkategorie_name)
        else:
            excel_id: int = int(excel_id)
        english_subkategorie_name: str = self._get_english_name_by_excel_id(excel_id=excel_id)
        return english_subkategorie_name


class CategoryLoader:

    def __init__(self):
        self.db_master = CombDBService()

    def insert_subkategorien_data_source_allocation(self, cat_id: int, herkunft_id: int) -> None:
        self.db_master.insert_subcategory_datenherkunft(cat_id, herkunft_id)

    def insert_oberkategorien_data_source_allocation(self):
        pass
