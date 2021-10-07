class NoExcelIdFoundForGermanCategoryName(Exception):
    def __init__(self, german_category_name: str):
        super().__init__("Es konnte keine Excel-ID fuer die Kategorie {} gefunden werden".format(german_category_name))