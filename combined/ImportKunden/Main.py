from combined.ImportKunden.ImportKunden import ImportKunden
from combined.ImportKunden.InsertKunden import InsertKunden

importer = ImportKunden()
#kunden_liste = importer.start_import()
#inserter = InsertKunden()
#inserter.insert_kunden(kunden_liste)
importer.findMatchingID()


#TODO Pfad richtig angeben, damit die Mitarbeiter auch die Funktion nutzen können
#TODO Geburtsdatum Spalte anlegen (nur per Hand)
#TODO wie Vor- und Nachname an SQL übergeben, die sind doch da