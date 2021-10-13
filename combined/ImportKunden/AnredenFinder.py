import csv

import config


class AnredenFinder:


    def __init__(self):
        self.geschlechts_map = []
        # open file for reading
        with open(config.VORNAMEN_GENDER_CSV, encoding='UTF8') as csvDataFile:
            # read file as csv file
            csvReader = csv.reader(csvDataFile)

            # for every row, write the name and gender in map
            for row in csvReader:
                pair = {row[0].upper(): row[1]}
                self.geschlechts_map.append(pair)

    def finde_geschlecht(self, nameToTest):
        nameToTest = nameToTest.upper()
        for name in self.geschlechts_map:
            if name.get(nameToTest):
                return name.get(nameToTest)
        return "undefiniert"

    def finde_Anrede(self, nameToTest):
        if(self.finde_geschlecht(nameToTest)=='männlich'):
            return 'Herr'
        elif (self.finde_geschlecht(nameToTest)=='weiblich'):
            return 'Frau'
        else:
            return 'k.A.'