import csv


class genderFinder:
    global genderMap
    genderMap = []

    def __init__(self):
        # open file for reading
        with open('vornamen_gender.csv', encoding='UTF8') as csvDataFile:
            # read file as csv file
            csvReader = csv.reader(csvDataFile)

            # for every row, write the name and gender in map
            for row in csvReader:
                dict1 = {row[0]: row[1]}
                genderMap.append(dict1)

    def checkGender(self, nameToTest):
        for csvDatum in genderMap:
            if csvDatum.get(nameToTest):
                return csvDatum.get(nameToTest)
        return "undefiniert"


finder = genderFinder()
print(finder.checkGender("Max"))
