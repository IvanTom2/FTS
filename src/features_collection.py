from abc import ABC
import re
from collections import namedtuple
import math

Sign = namedtuple("Sign", ["name", "weight", "postscript"])
Rx = namedtuple("Rx", ["sign", "rx"])


class AbstractFeature(ABC):
    def __init__(self, v) -> None:
        self.standard_value = v


class NumericalFeature(AbstractFeature):
    SIGN: list[Sign]
    RXS: list[Rx]
    BASE: Sign
    NAME: str

    def __init__(self, value: str, sign: Sign) -> None:
        self.original_value = value
        self.standard_value = self._standardization(value, sign)

    def _isclose(self, value: float) -> float:
        if math.isclose(value, round(value)):
            value = round(value)
        return value

    def _determine_kf(self, sign: Sign):
        if self.BASE.name == sign.name:
            return 1
        return self._isclose(sign.weight / self.BASE.weight)

    def _standardization(self, value: str, sign: Sign):
        num_value = re.search("\d*[.,]?\d+", value)[0]
        num_value = num_value.replace(",", ".")
        num_value = float(num_value)

        kf = self._determine_kf(sign)
        num_value = self._isclose(num_value * kf)

        return num_value

    def __eq__(self, other: AbstractFeature) -> bool:
        if isinstance(other, self.__class__):
            if self.standard_value == other.standard_value:
                return True
        return False

    def __hash__(self) -> int:
        return hash(self.standard_value)

    def __repr__(self) -> str:
        return rf"{self.NAME} = {self.standard_value}"

    def __str__(self) -> str:
        return rf"{self.NAME} = {self.standard_value}"


class Weight(NumericalFeature):
    SIGN = [
        Sign("Мкг", 0.000001, r"мкг|микрограмм|µg|microgram"),
        Sign("Мг", 0.001, r"мг|миллиграмм|mg|milligram"),
        Sign("Гр", 1, r"г|гр|грамм|g|gram"),
        Sign("Кг", 1000, r"кг|килограмм|kg|kilogram"),
    ]

    RXS = [Rx(s, rf"\d*[.,]?\d+\s*(?:{s.postscript})") for s in SIGN]
    BASE = SIGN[2]
    NAME = "Weight"


class Volume(NumericalFeature):
    SIGN = [
        Sign("Мл", 0.001, r"мл|миллилитр|ml|milliliter"),
        Sign("Л", 1, r"литр|л|liter|l"),
    ]

    RXS = [Rx(s, rf"\d*[.,]?\d+\s*(?:{s.postscript})") for s in SIGN]
    BASE = SIGN[1]
    NAME = "Volume"


class MemoryCapacity(NumericalFeature):
    SIGN = [
        Sign("КБ", 0.000001, r"kb|kilobite|килобайт|кб"),
        Sign("МБ", 0.001, r"mb|megabite|мегабай|мб"),
        Sign("ГБ", 1, r"gb|gigabite|гигабайт|гб"),
        Sign("ТБ", 1000, r"tb|terabite|терабайт|тб"),
    ]

    RXS = [Rx(s, rf"\d*[.,]?\d+\s*(?:{s.postscript})") for s in SIGN]
    BASE = SIGN[1]
    NAME = "Memory Capacity"


ALL_FUTURES: list[NumericalFeature] = [Weight, Volume, MemoryCapacity]


if __name__ == "__main__":
    txt1 = "100 г"
    txt2 = "0.1 кг"

    w1 = Weight(txt1, Sign("Гр", 1, "г"))
    w2 = Weight(txt2, Sign("Кг", 1000, "кг"))

    v = Volume(txt1, Sign("Гр", 1, "г"))

    print(w1 == v)
