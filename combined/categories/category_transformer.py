from combined.categories.category_extractor import CategoryExtractor


class CategoryTransformer:

    def __init__(self):
        self.category_extractor = CategoryExtractor()
        self.category_extractor.load_data()
        self.category_translator = Translator(german_csv=self.category_extractor.german_category_csv,
                                              english_csv=self.category_extractor.english_category_csv)

    def _save_oberkategorie_mapping_to_file(self) -> None:
        pass

    def _save_subkategorie_mapping_to_file(self) -> None:
        pass

    def _is_subkategorie_mappable(self, english_subkategorie_name: str) -> bool:
        pass

    def _is_oberkategorie_mappable(self, english_oberkategorie_name: str) -> bool:
        pass

    def _map_oberkategorien(self):
        # {'PRODUKTOBERKATEGORIE_ID': 1, 'BEZEICHNUNG': 'Lebensmittel'}
        # {'product_class_id': '1', 'product_subcategory': 'Nüsse', 'product_category': 'Spezialität', 'product_department': 'Erzeugnis', 'product_family': 'Food'}
        pass

    def _map_subkategorien(self):
        # {'PRODUKTKATEGORIE_ID': 262, 'BEZEICHNUNG': 'Nüsse', 'ALTERFREIGABE': 0}
        # {'product_class_id': '1', 'product_subcategory': 'Nüsse', 'product_category': 'Spezialität', 'product_department': 'Erzeugnis', 'product_family': 'Food'}
        for subkategorie_entry in self.category_extractor.subkategorien:
            translated_name: str = self.category_translator.translate(
                german_subkategorie_name=subkategorie_entry.get("BEZEICHNUNG"))
            if not self._is_subkategorie_mappable(translated_name):
                # TODO Kategorie einfügen
                pass
            # TODO Zuordnung in Zuweisungstabelle speichern
            # TODO Alte und Neue Ids in CSV speichern


class Translator:
    def __init__(self, german_csv: list[dict], english_csv: list[dict]):
        self.german_csv = german_csv
        self.english_csv = english_csv

    def _get_excel_id_by_translated_subcategory(self, german_subcategory: str) -> int:
        for german_category_csv in self.german_csv:
            if german_subcategory == german_category_csv.get("product_family"):
                return german_category_csv.get("product_class_id")

    def _get_english_name_by_excel_id(self, excel_id: int) -> str:
        for original_csv_category in self.english_csv:
            if excel_id == int(original_csv_category.get("product_class_id")):
                return original_csv_category.get("product_subcategory")

    def translate(self, german_subkategorie_name: str) -> str:
        excel_id: int = self._get_excel_id_by_translated_subcategory(german_subcategory=german_subkategorie_name)
        english_subkategorie_name: str = self._get_english_name_by_excel_id(excel_id=excel_id)
        return english_subkategorie_name
