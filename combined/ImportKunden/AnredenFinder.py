import csv


class AnredenFinder:
    global geschlechts_map
    geschlechts_map = []

    def __init__(self):
        # open file for reading
        with open('vornamen_gender.csv', encoding='UTF8') as csvDataFile:
            # read file as csv file
            csvReader = csv.reader(csvDataFile)

            # for every row, write the name and gender in map
            for row in csvReader:
                dict1 = {row[0]: row[1]}
                geschlechts_map.append(dict1)

    def finde_geschlecht(self, nameToTest):
        for csvDatum in geschlechts_map:
            if csvDatum.get(nameToTest):
                return csvDatum.get(nameToTest)
        return "undefiniert"

    def finde_Anrede(self, nameToTest):
        if(self.finde_geschlecht(nameToTest)=='männlich'):
            return 'Herr'
        elif (self.finde_geschlecht(nameToTest)=='weiblich'):
            return 'Frau'
        else:
            return 'Herr / Frau'

