import random
from main import _str_time_prop


class Util:
    def __init__(self):
        pass

    @staticmethod
    def oz_to_ibs(oz: float) -> float:
        return ((oz * 28.35) / 1000) * 2

    @staticmethod
    def number_str_to_float(incorrect_value: str) -> float:
        correct_value = incorrect_value.replace(",", ".")
        return float(correct_value)

    @staticmethod
    def cm_to_inch(cm: float) -> float:
        return cm / 2.54

    def get_random_price(self, original_price: float) -> float:
        return original_price * self.generate_deviation_factor()

    @staticmethod
    def generate_deviation_factor() -> float:
        return random.uniform(0.8, 1.2)

    @staticmethod
    def random_date(start, end, prop) -> str:
        return _str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)
