from typing import Optional

from combined.categories.category_extractor import CategoryExtractor


class CategoryTransformer:

    def __init__(self):
        self.category_loader = CategoryExtractor()

    def _save_oberkategorie_mapping_to_file(self):
        pass

    def _save_subkategorie_mapping_to_file(self):
        pass

    def _is_subkategorie_mappable(self):
        pass

    def _is_oberkategorie_mappable(self):
        pass

    def _translate_name(self, original_name: str, language: Optional[str]) -> str:
        # Sprache spezifiziert
        # Sprache herausfinden
        # API Call
        pass

    def _map_oberkategorien(self):
        pass

    def _map_subkategorien(self):
        pass
