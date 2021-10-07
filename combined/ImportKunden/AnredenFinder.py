import csv

import config


class AnredenFinder:

    def __init__(self):
        self.geschlechts_map = []
        # open file for reading
        with open(config.VORNAMEN_GENDER_CSV, encoding='UTF8') as csvDataFile:
            # read file as csv file
            csv_reader = csv.reader(csvDataFile)

            # for every row, write the name and gender in map
            for row in csv_reader:
                dict1 = {row[0].upper(): row[1]}
                self.geschlechts_map.append(dict1)

    def finde_geschlecht(self, name_to_test):
        name_to_test = name_to_test.upper()
        for name in self.geschlechts_map:
            if name.get(name_to_test):
                return name.get(name_to_test)
        return "undefiniert"

    def finde_anrede(self, name_to_test):
        if self.finde_geschlecht(name_to_test) == 'männlich':
            return 'Herr'
        elif self.finde_geschlecht(name_to_test) == 'weiblich':
            return 'Frau'
        else:
            return 'k.A.'
