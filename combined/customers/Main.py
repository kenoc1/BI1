from combined.customers.ImportKunden import ImportKunden
from combined.customers.InsertKunden import InsertKunden

if __name__ == "__main__":
    importer = ImportKunden()
    kunden_liste = importer.start_import()
    inserter = InsertKunden()
    inserter.insert_kunden(kunden_liste)
