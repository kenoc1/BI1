# Produkte aus DB holen und in ein Array schreiben
# ProduktFinder initialisieren und Funktion aufrufen
from combined.productImageFinder.imageFinder import ImageFinder
from combined.productImageFinder.imageInserter import ImageInserter
from combined.productImageFinder.productImporter import ProductImporter
from combined.productImageFinder.productNameCutter import ProductNameCutter

i = 0
productFinder = ProductImporter()
importedProducts = productFinder.getProducts()

# Produktnamen für die Suche anpassen (Marke entfernen)
productCutter = ProductNameCutter()
correctedProductNames = productCutter.cutProductNames(importedProducts)
print('---Produktnamen wurden korregiert---')

#Für jedes Produkt Links suchen und es dann in die Datenbank senden
imageFinder = ImageFinder()
imageInserter = ImageInserter()
counter = 1
for product in correctedProductNames:
    print(product)
    productWithURL = imageFinder.addImageUrl(product)
    imageInserter.insertImageUrls(productWithURL)
    print(f'Produkt {counter} von {str(len(correctedProductNames))}')
    counter = counter + 1
imageFinder.saveFailedProducts()

