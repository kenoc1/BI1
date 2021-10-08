class NoExcelIdFoundForGermanCategoryName(Exception):
    def __init__(self, german_category_name: str):
        super().__init__("Es konnte keine Excel-ID fuer die Kategorie {} gefunden werden".format(german_category_name))


class NoCombIDFoundForF2IDException(Exception):
    def __init__(self, old_id: int):
        super().__init__(
            "No comb_id was found for given old_id {}".format(old_id))
