def uniform_string(string: str) -> str:
    without_space: str = string.replace(" ", "")
    in_lowercase: str = without_space.lower()
    return in_lowercase


def compare_strings(first: str, second: str) -> bool:
    return uniform_string(first) == uniform_string(second)

if __name__ == "__main__":
    pass
