def uniform_string(string: str) -> str:
    without_space: str = string.replace(" ", "")
    in_lowercase: str = without_space.lower()
    return in_lowercase


def compare_strings(first, second) -> bool:
    return uniform_string(str(first)) == uniform_string(str(second))


def datetime_to_date_string(datetime) -> str:
    return str(datetime).split()[0]


if __name__ == "__main__":
    pass
