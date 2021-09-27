class StringEqualityTester:

    def __init__(self):
        pass

    def _uniform_string(self, string: str) -> str:
        without_space: str = string.replace(" ", "")
        in_lowercase: str = without_space.lower()
        return in_lowercase

    def compare(self, first: str, second: str) -> bool:
        return self._uniform_string(first) == self._uniform_string(second)
