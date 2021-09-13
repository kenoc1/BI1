from datetime import datetime
import csv
from db_interact import DB

db = DB()


def create_lagereinheiten_into_database():
    for regal in range(1, 61):  # 60
        print()
        for gemueseauslage in range(0, 11): # 10
            for spalte in range(0, 6):
                print()
        for obstkorb in range(0, 3): # 2
            print()

        # regale
