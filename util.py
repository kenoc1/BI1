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
