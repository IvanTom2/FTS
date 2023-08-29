from abc import ABC
import re
from collections import namedtuple
import math
from decimal import Decimal


Sign = namedtuple("Sign", ["name", "weight", "postscript"])
Rx = namedtuple("Rx", ["sign", "rx"])


class Measure(object):
    def __init__(
        self,
        name: str,
        weight: float,
        designation: str,
        regex: str = "",
    ) -> None:
        self.name = name
        self.weight = weight
        self.designation = designation
        self.regex = regex


class FeatureMeasures(object):
    def __init__(
        self,
        measures: list[Measure],
        numerical_rx: str = "\d*[.,]?\d+\s*",
        prefix: str = "",
        postfix: str = "",
    ) -> None:
        self.numerical_regex = numerical_rx
        self.prefix = prefix
        self.postfix = postfix
        self.measures = self._prepare(measures)

        self.__index = 0

    def _make_regex(self, measure: Measure) -> str:
        return f"{self.postfix}{self.numerical_regex}(?:{measure.designation}){self.postfix}"

    def _prepare(
        self,
        measures: list[Measure],
    ) -> list[Measure]:
        for measure in measures:
            measure.weight = Decimal(str(measure.weight))

            if not measure.regex:
                measure.regex = self._make_regex(measure)
        return measures

    def __iter__(self):
        while self.__index < len(self.measures):
            yield self.measures[self.__index]
            self.__index += 1


class Type(object):
    def __init__(
        self,
        name: str,
        designation: str,
    ) -> None:
        self.name = name
        self.designation = designation


class FeatureTypes(object):
    def __init__(
        self,
        types: list[Type],
        prefix: str = "",
        postfix: str = "",
    ) -> None:
        self.prefix = prefix
        self.postfix = postfix
        self.types = self._prepare(types)

        self.__index = 0

    def _make_regex(self, type_: Type) -> Type:
        return f"{type_}"

    def _prepare(self, types: list[Type]) -> Type:
        return [self._make_regex(type_) for type_ in types]

    def __iter__(self):
        while self.__index < len(self.types):
            yield self.types[self.__index]
            self.__index += 1


class AbstractFeature(ABC):
    def __init__(self, v) -> None:
        self.standard_value = v


class StringFeatures(AbstractFeature):
    TYPES: FeatureTypes
    NAME: str

    def __init__(
        self,
        value: str,
        type_: Type,
    ) -> None:
        self.original_value = value
        self.standard_value = self._standartization(type_)

    def _standartization(self, type_: Type) -> str:
        return type_.name

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


class NumericalFeature(AbstractFeature):
    MEASURES: FeatureMeasures
    NAME: str

    def __init__(self, value: str, measure: Measure) -> None:
        self.original_value = value
        self.standard_value = self._standartization(value, measure)

    def _standartization(self, value: str, measure: Measure):
        num_value = re.search(r"\d*[.,]?\d+", value)[0]
        num_value = num_value.replace(",", ".")
        num_value = Decimal(num_value)

        kf = measure.weight
        num_value = num_value * kf

        # TODO: решить, оставить ли Decimal
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
    MEASURES = FeatureMeasures(
        measures=[
            Measure("Микрокилограмм", 0.000001, r"мкг|микрограмм|µg|microgram"),
            Measure("Миллиграмм", 0.001, r"мг|миллиграмм|mg|milligram"),
            Measure("Грамм", 1, r"г|гр|грамм|g|gram"),
            Measure("Килограмм", 1000, r"кг|килограмм|kg|kilogram"),
        ],
    )
    NAME = "Weight"


class Volume(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure("Миллилитр", 0.001, r"мл|миллилитр|ml|milliliter"),
            Measure("Литр", 1, r"литр|л|liter|l"),
        ],
    )
    NAME = "Volume"


class MemoryCapacity(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure("Килобайт", 0.000001, r"kb|kilobite|килобайт|кб"),
            Measure("Мегабайт", 0.001, r"mb|megabite|мегабай|мб"),
            Measure("Гигабайт", 1, r"gb|gigabite|гигабайт|гб"),
            Measure("Террабайт", 1000, r"tb|terabite|терабайт|тб"),
        ],
    )
    NAME = "Memory Capacity"


ALL_FUTURES: list[NumericalFeature] = [Weight, Volume, MemoryCapacity]


if __name__ == "__main__":
    pass
