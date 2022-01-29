from bing_images import bing

from combined.ImportKunden.csvWriter import CsvWriter


class ImageFinder:
    failedProducts = []

    def findProductPackage(self, term):
        try:
            url = bing.fetch_image_urls(term, limit=3, filters='+filterui:licenseType-Any')
            return url
        except TypeError:
            self.failedProducts.append(term)
            print("Fehler aufgetreten!")
            return "TypeError"

    def addImageUrl(self, product):
        foundImages = self.findProductPackage(str(product[0]))
        if len(foundImages) == 0:
            self.failedProducts.append(product)
            product.append('Kein Bild')
        else:
            self.removeSingleQuote(foundImages)
            linked = ','.join(foundImages)
            if (len(linked) > 1000):
                self.failedProducts.append(product)
                product.append('Länge der Links zu lang')
            else:
                product.append(linked)
        print('Produkt ' + str(product[0]) + ' wurde ergänzt')
        return product

    def saveFailedProducts(self):
        csvWriter = CsvWriter()
        csvWriter.writeToCsv(self.failedProducts)

    def removeSingleQuote(self, foundLinks):
        for foundLink in foundLinks:
            if("/'" in foundLink):
                foundLinks.remove(foundLink)
        return foundLinks



