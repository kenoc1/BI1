from combined.categories.category_extractor import CategoryExtractor
from combined.custom_exceptions import NoExcelIdFoundForGermanCategoryName
from combined.string_equality_tester import compare
from db_service import DB_MASTER


class CategoryTransformer:

    def __init__(self):
        self.db_master = DB_MASTER()
        self.category_extractor = CategoryExtractor()
        self.category_extractor.load_data()
        self.category_translator = _Translator(german_csv=self.category_extractor.german_category_csv,
                                               english_csv=self.category_extractor.english_category_csv)

    def _save_oberkategorie_mapping_to_file(self) -> None:
        pass

    def _save_subkategorie_mapping_to_file(self) -> None:
        pass

    def _is_subkategorie_mappable(self, english_subkategorie_name: str) -> bool:
        """
        Assesses whether a subcategory from Filiale2 can be unambiguously mapped to a subcategory from Onlineshop.
        :param english_subkategorie_name: The english name of a subcategory that is assessed.
        :return: True if there is exactly one possible mapping, else False
        """
        selected_entries = self.db_master.select_subcat_where_bezeichnung(english_subkategorie_name)
        return True if len(selected_entries) == 1 else False

    def _is_oberkategorie_mappable(self, english_oberkategorie_name: str) -> bool:
        pass

    def _map_oberkategorien(self):
        # DB:  {'PRODUKTOBERKATEGORIE_ID': 1, 'BEZEICHNUNG': 'Lebensmittel'}
        # CSV: {'product_class_id': '1', 'product_subcategory': 'Nüsse', 'product_category': 'Spezialität', 'product_department': 'Erzeugnis', 'product_family': 'Food'}
        pass

    def _map_subkategorien(self):
        # DB:  {'PRODUKTKATEGORIE_ID': 262, 'BEZEICHNUNG': 'Nüsse', 'ALTERFREIGABE': 0}
        # CSV: {'product_class_id': '1', 'product_subcategory': 'Nüsse', 'product_category': 'Spezialität', 'product_department': 'Erzeugnis', 'product_family': 'Food'}
        for subkategorie_entry in self.category_extractor.f2_db_subkategorien:
            try:
                translated_name: str = self.category_translator.translate(
                    german_subkategorie_name=subkategorie_entry.get("BEZEICHNUNG"))
                if not self._is_subkategorie_mappable(translated_name):
                    # TODO Kategorie einfügen
                    pass
                elif True:  # TODO Wenn nicht schon vorhanden, also zugewiesen ist
                    # TODO Zuordnung in Zuweisungstabelle speichern
                    pass
                # TODO Alte und Neue Ids in CSV speichern
            except NoExcelIdFoundForGermanCategoryName:
                print("Not found: {}".format(subkategorie_entry.__str__()))

                pass
                # TODO Es kann keine Übersetzung gefunden werden

    def run(self):
        self._map_subkategorien()


class _Translator:
    def __init__(self, german_csv: list[dict], english_csv: list[dict]):
        self.german_csv = german_csv
        self.english_csv = english_csv

    def _get_excel_id_by_translated_subcategory(self, german_subcategory: str) -> int:
        for german_category_csv_entry in self.german_csv:
            if compare(german_subcategory, german_category_csv_entry.get("product_subcategory")):
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


ct = CategoryTransformer()
ct.run()
