import time
from random import random, seed, gauss, randint, uniform
import config
import names
from datetime import timedelta, datetime
import math


def _str_time_prop(start, end, time_format, prop) -> str:
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def generate_mail_with_company_name(company_name: str):
    return f"info@{company_name}.net"


def generate_firstname() -> str:
    return names.get_first_name()


def generate_lastname() -> str:
    return names.get_last_name()


def random_date(start, end, prop) -> str:
    return _str_time_prop(start, end, config.TIME_FORMAT_F2, prop)


def random_salary() -> float:
    return uniform(300, 8000)


def random_commission_rate() -> float:
    return round(uniform(0, 5), 2)


def generate_delivery_costs():
    return uniform(3, 70)


def generate_delivery_date(sale_date):
    return (datetime.strptime(sale_date, config.TIME_FORMAT_F2) + timedelta(days=randint(3, 5))).strftime(
        config.TIME_FORMAT_F2)


def generate_adjustment_date(sale_date):
    return (datetime.strptime(sale_date, config.TIME_FORMAT_F2) + timedelta(days=14)).strftime(config.TIME_FORMAT_F2)


def generate_given_money(sale_sum):
    return roundup(sale_sum)


def generate_payment_method():
    return config.PAYMENT_METHODS_F2[randint(0, 1)]


def roundup(x):
    return int(math.ceil(x / 10.0)) * 10


def get_random_price(original_price: float) -> float:
    return original_price * generate_deviation_factor()


def generate_deviation_factor() -> float:
    return uniform(0.8, 1.2)


def random_date_for_priceloader(start, end, prop) -> str:
    return _str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)


def oz_to_ibs(oz: float) -> float:
    return ((oz * 28.35) / 1000) * 2


def number_str_to_float(incorrect_value: str) -> float:
    correct_value = incorrect_value.replace(",", ".")
    return float(correct_value)


def cm_to_inch(cm: float) -> float:
    return cm / 2.54


class Util:
    def __init__(self):
        print("util init...")
        # sale_date = random_date('12/15/2020 1:30 PM', '1/15/2021 4:50 AM', random())
        # delivery_date = generate_delivery_date(sale_date)
        # adjustment_date = generate_adjustment_date(sale_date)
        # print(sale_date)
        # print(delivery_date)
        # print(adjustment_date)
