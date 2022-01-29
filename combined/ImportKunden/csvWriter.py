import csv
class CsvWriter:
    def writeToCsv(self, array):
        with open('failedProducts.csv', 'w', encoding='UTF8') as f:
            writer = csv.writer(f)
            for product in array:
                writer.writerow(product)