import time
from random import random, seed, gauss, randint, uniform
import config
import cx_Oracle
import names


def generate_firstname() -> str:
    return names.get_first_name()


def generate_lastname() -> str:
    return names.get_last_name()


def _str_time_prop(start, end, time_format, prop) -> str:
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop) -> str:
    return _str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)


class Util:
    def __init__(self):
        print("util init")
