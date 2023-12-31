from abc import ABC
import regex as re
from collections import namedtuple
from decimal import Decimal


class FeatureValidationMode(object):
    STRICT = "strict"
    MODEST = "modest"
    CLIENT = "client"
    SOURCE = "source"


class FeatureNotFoundMode(object):
    STRICT = "strict"
    MODEST = "modest"


class Type(object):
    def __init__(
        self,
        name: str,
        designation: str,
        regex: str = "",
    ) -> None:
        self.name = name
        self.designation = designation
        self.regex = regex


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
        return f"{self.prefix}{type_.designation}{self.postfix}"

    def _prepare(self, types: list[Type]) -> Type:
        for type_ in types:
            if not type_.regex:
                type_.regex = self._make_regex(type_)
        return types

    def __iter__(self):
        while self.__index < len(self.types):
            yield self.types[self.__index]
            self.__index += 1


class AbstractFeature(ABC):
    TYPES: FeatureTypes
    NAME: str
    VALIDATION_MODE: FeatureValidationMode
    NOT_FOUND_MODE: FeatureNotFoundMode
    PRIORITY: int = 10

    def __init__(self, v) -> None:
        self.standard_value = v

    @classmethod
    @property
    def designations(self):
        pass


class NotFoundStatus(object):
    ACCEPT = "accept"
    DROP = "drop"

    def __init__(
        self,
        feature_set1: set,
        feature_set2: set,
        not_found_mode: FeatureNotFoundMode,
        feature_name: str,
    ):
        self.empty1 = self._is_empty(feature_set1)
        self.empty2 = self._is_empty(feature_set2)
        self.not_found_mode = not_found_mode
        self.feature_name = feature_name

        self.both_not_found = True if self.empty1 and self.empty2 else False
        self.one_not_found = True if self.empty1 or self.empty2 else False

    def _is_empty(self, feature_set: set) -> bool:
        if len(feature_set) == 0:
            return True
        return False

    def set_decision(self, not_found_mode) -> int:
        if not_found_mode == FeatureNotFoundMode.MODEST:
            self.desicion = 1
        else:  # not_found_mode == FeatureNotFoundMode.STRICT
            self.desicion = 0

    @property
    def desicion(self) -> int:
        if self.both_not_found:
            return 1
        elif self.one_not_found:
            if self.not_found_mode == self.ACCEPT:
                return 1
            else:  # self.not_found_mode == self.DROP:
                return 0

    @property
    def status(self) -> str:
        if self.both_not_found:
            return f"Not found both {self.feature_name}: decision {self.desicion}; "
        elif self.one_not_found:
            which_not_found = "client" if self.empty1 else "source"
            return f"Not found {which_not_found} {self.feature_name}: desicion {self.desicion}; "

    def __bool__(self) -> bool:
        return self.one_not_found


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
        numerical_rx: str = r"\d*[.,]?\d+\s*",
        prefix: str = "",
        postfix: str = "",
    ) -> None:
        self.numerical_regex = numerical_rx
        self.prefix = prefix
        self.postfix = postfix
        self.measures = self._prepare(measures)

        self.__index = 0

    def _make_regex(self, measure: Measure) -> str:
        return f"{self.prefix}({self.numerical_regex}(?:{measure.designation})){self.postfix}"

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


class StringFeature(AbstractFeature):
    TYPES: FeatureTypes
    NAME: str
    VALIDATION_MODE: FeatureValidationMode
    NOT_FOUND_MODE: FeatureNotFoundMode
    PRIORITY: int = 10

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

    @classmethod
    @property
    def designations(self):
        return self.TYPES


class Color(StringFeature):
    TYPES = FeatureTypes(
        types=[
            Type("Черный", r"black|ч[её]рн"),
            Type("Белый", r"white|бел"),
            Type("Синий", r"blue|син"),
            Type("Зеленый", r"green|зелен"),
            Type("Красный", r"red|красн"),
            Type("Желтый", r"yellow|ж[её]лт"),
            Type("Коричневый", r"brown|коричн"),
        ]
    )

    NAME = "Color"
    VALIDATION_MODE = FeatureValidationMode.STRICT
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class FarmaForm(StringFeature):
    TYPES = FeatureTypes(
        types=[
            Type("Таблетка", r"таб|тб[.]|тб\b|тбл"),
            Type("Драже", r"драж|др[.]"),
            Type("Гранула", r"гран"),
            Type("Порошок", r"порош"),
            Type("Капсула", r"капс"),
            Type("Сбор", r"сбор"),
            Type("Карамель", r"карам"),
            Type("Карандаш", r"каранд"),
            Type("Крем", r"крем"),
            Type("Мазь", r"мазь"),
            Type("Гель", r"гель"),
            Type("Суппозитории", r"супп|свеч"),
            Type("Паста", r"паст"),
            Type("Капли", r"кап\.|кап\b|капл"),
            Type("Настойка", r"наст"),
            Type("Сироп", r"сироп"),
            Type("Суспензия", r"сусп"),
            Type("Эмульсия", r"эмул"),
        ]
    )

    NAME = "FarmaForm"
    VALIDATION_MODE = FeatureValidationMode.STRICT
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class NumericalFeature(AbstractFeature):
    MEASURES: FeatureMeasures
    NAME: str
    VALIDATION_MODE: FeatureValidationMode
    NOT_FOUND_MODE: FeatureNotFoundMode
    PRIORITY: int = 10

    def __init__(self, value: str, measure: Measure) -> None:
        self.original_value = value
        self.standard_value = self._standartization(value, measure)

    def _standartization(self, value: str, measure: Measure):
        # num_value = re.search(r"\d*[.,]?\d+", value)[0]
        num_value = re.search(r"\d+[.,]?\d*", value)[0]
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

    @classmethod
    @property
    def designations(self):
        return self.MEASURES


class Weight(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure(
                "Микрокилограмм",
                0.000001,
                r"мкг[^\\\/]|микрограмм[^\\\/]|µg[^\\\/]|microgram[^\\\/]",
            ),
            Measure(
                "Миллиграмм",
                0.001,
                r"мг[^\\\/]|миллиграмм[^\\\/]|mg[^\\\/]|milligram[^\\\/]",
            ),
            Measure(
                "Грамм",
                1,
                r"г[^\\\/а-я]|г[р.][^\\\/]|грамм[^\\\/]|g[^\\\/a-z]|g[r.][^\\\/]|gram[^\\\/]",
            ),
            Measure(
                "Килограмм",
                1000,
                r"кг[^\\\/]|килограмм[^\\\/]|kg[^\\\/]|kilogram[^\\\/]",
            ),
        ],
    )

    NAME = "Weight"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class Volume(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure("Миллилитр", 0.001, r"мл|миллилитр|ml|milliliter"),
            Measure("Литр", 1, r"литр|л\b|liter|l\b"),
        ],
        prefix=r"[^\\\/]",
    )

    NAME = "Volume"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class MemoryCapacity(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure("Килобайт", 0.000001, r"kb|kilobite|килобайт|кб"),
            Measure("Мегабайт", 0.001, r"mb|megabite|мегабайт|мб"),
            Measure("Гигабайт", 1, r"gb|gigabite|гигабайт|гб"),
            Measure("Террабайт", 1000, r"tb|terabite|терабайт|тб"),
        ],
    )

    NAME = "Memory Capacity"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class Quantity(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure(
                "Количество",
                1,
                "",
                r"([2-9][,.]?\d*|[1-9]\d+[,.]?\d*)\s*(?:шт|уп|пач|доз)",
            ),
            Measure(
                "Нумерованное количество",
                1,
                "",
                r"(?:[^а-яa-z]n|№|[^а-яa-z]x|[^а-яa-z]х)\s*([2-9][,.]?\d*|[1-9]\d+[,.]?\d*)",
            ),
        ],
    )

    NAME = "Quantity"
    VALIDATION_MODE = FeatureValidationMode.STRICT
    NOT_FOUND_MODE = FeatureNotFoundMode.STRICT


class Size(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure("Размер1", 1, r"р[.]|\bр|размер"),
            Measure("Размер2", 1, "", r"(?:р[.]|\bр)\s*\d*[.,]?\d+"),
        ],
    )

    NAME = "Size"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class ConcentrationPerDose(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure("Мкг на дозу", 0.001, r"мкг[\\\/](?:доз|сут)"),
            Measure("Мг на дозу", 1, r"мг[\\\/](?:доз|сут)"),
            Measure("Г на дозу", 1000, r"г[\\\/](?:доз|сут)"),
        ],
    )

    NAME = "ConcentrationPerDose"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class Length(NumericalFeature):
    MEASURES = FeatureMeasures(
        measures=[
            Measure(
                "Миллиметр",
                0.001,
                r"мм|миллиметр|mm|millimeter",
                # regex=r"(?<!(?:x|х|на)\s*)\b(\d*[.,]?\d+\s*(?:мм|миллиметр|mm|millimeter))\b(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:мм|миллиметр|mm|millimeter))",
                # regex=r"(?<!(?:x|х|на)\s*)(?:\b|$)(\d*[.,]?\d+\s*(?:мм|миллиметр|mm|millimeter))(?:\b|$)(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:мм|миллиметр|mm|millimeter))",
            ),
            Measure(
                "Сантиметр",
                0.01,
                r"см|сантиметр|centimeter|cm",
                # regex=r"(?<!(?:x|х|на)\s*)\b(\d*[.,]?\d+\s*(?:см|сантиметр|centimeter|cm))\b(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:см|сантиметр|centimeter|cm))",
                # regex=r"(?<!(?:x|х|на)\s*)(?:\b|$)(\d*[.,]?\d+\s*(?:см|сантиметр|centimeter|cm))(?:\b|$)(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:см|сантиметр|centimeter|cm))",
            ),
            Measure(
                "Метр",
                1,
                r"м\b|метр|m\b|meter",
                # regex=r"(?<!(?:x|х|на)\s*)\b(\d*[.,]?\d+\s*(?:м[^м]|метр|m[^m]|meter))\b(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:м[^м]|метр|m[^m]|meter))",
                # regex=r"(?<!(?:x|х|на)\s*)(?:\b|$)(\d*[.,]?\d+\s*(?:м[^м]|метр|m[^m]|meter))(?:\b|$)(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:м[^м]|метр|m[^m]|meter))",
            ),
            Measure(
                "Километр",
                1000,
                r"км|километр|km|kilometer",
                # regex=r"(?<!(?:x|х|на)\s*)\b(\d*[.,]?\d+\s*(?:км|километр|km|kilometer))\b(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:км|километр|km|kilometer))",
                # regex=r"(?<!(?:x|х|на)\s*)(?:\b|$)(\d*[.,]?\d+\s*(?:км|километр|km|kilometer))(?:\b|$)(?!.*(?:x|х|на)\s*\d*[.,]?\d+\s*(?:км|километр|km|kilometer))",
            ),
        ],
    )

    NAME = "Length"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST


class Designation(object):
    def __init__(self, original_value: str) -> None:
        self.original_value = original_value
        self.num_value = self._get_num_value(original_value)

        self.checked = False
        self.weight = None
        self.standard_value = self.num_value

    def _get_num_value(self, value: str) -> Decimal:
        num_value: str = re.search(r"\d*[.,]?\d+", value)[0]
        num_value = Decimal(num_value.replace(",", "."))
        return num_value

    def set_weight(self, weight: float) -> None:
        self.weight = weight
        self.checked = True

    def set_standard_value(self, potential_weight: float):
        """potential weight is using if designation don't have weight"""
        if self.have_weight():
            self.standard_value = self.num_value * Decimal(str(self.weight))
        else:
            self.standard_value = self.num_value * Decimal(str(potential_weight))
        return self

    def have_weight(self) -> bool:
        if self.weight is None:
            return False
        return True

    @property
    def value(self):
        return self.original_value

    def __repr__(self) -> str:
        return rf"{self.original_value}; {self.weight}; {self.standard_value}"


class Dimension(AbstractFeature):
    _num = r"\d*[.,]?\d+"
    _sep_ = r"[xх]|на"
    _sep = rf"(?:{_sep_})"
    _sgn = r"(?:см|cm|мм|mm|м|m)?"

    _weights = [
        (0.001, "мм|mm"),
        (0.01, "см|cm"),
        (1, r"m([^m]|\b)|м([^м]|\b)"),
    ]

    MEASURES: FeatureMeasures = FeatureMeasures(
        measures=[
            Measure(
                "n-размерность",
                weight=1,
                designation="",
                regex=rf"{_num}\s*{_sgn}\s*{_sep}\s*{_num}\s*{_sgn}(?:\s*{_sgn}\s*{_sep}\s*{_num}\s*{_sgn})*(?:\b|$)",
            )
        ],
    )

    NAME: str = "Dimension"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST
    PRIORITY = 1

    def __init__(
        self,
        value: str,
        measure: Measure,
    ) -> None:
        self.original_value = value
        self.standard_weight = 1

        self.standard_value = self._standartization(value)

    def _set_weight(self, designation: Designation) -> Designation:
        for weight in self._weights:
            srch = re.search(weight[1], designation.value, re.IGNORECASE)
            if srch:
                designation.set_weight(weight[0])
                break

        return designation

    def _set_value(self, designations: list[Designation]) -> list[Designation]:
        designations_with_weight = [d for d in designations if d.have_weight()]

        if not designations_with_weight:
            standard_weight = self.standard_weight
        else:
            standard_weight = designations_with_weight[-1].weight

        designations = [
            designation.set_standard_value(standard_weight)
            for designation in designations
        ]
        return designations

    def _standartization(self, value: str) -> set[Decimal]:
        designations = re.split(self._sep, value, re.IGNORECASE)
        designations = [Designation(dsgn) for dsgn in designations]
        designations = [self._set_weight(designation) for designation in designations]
        designations = self._set_value(designations)
        designations = frozenset([d.standard_value for d in designations])
        return designations

    def __eq__(self, other: AbstractFeature) -> bool:
        if isinstance(other, self.__class__):
            if self.standard_value == other.standard_value:
                return True
        return False

    def __hash__(self) -> int:
        return hash(self.standard_value)

    def __repr__(self) -> str:
        return "n-размерность = " + "x".join(
            list([str(v) for v in self.standard_value])
        )

    def __str__(self) -> str:
        return "n-размерность = " + "x".join(
            list([str(v) for v in self.standard_value])
        )

    @classmethod
    @property
    def designations(self):
        return self.MEASURES


class Concentration(AbstractFeature):
    _num1 = r"\d*[.,]?\d+"
    _num2 = r"\d*[.,]?\d*"
    _sep = r"[\\\/]"
    _top = r"(?:мкг|мг|г|кг)?"
    _bot = r"(?:мл|л)"

    _tops = [
        (0.000001, r"мкг"),
        (1, r"мг"),
        (1000, r"г"),
        (1000000, r"кг"),
    ]

    _bots = [
        (1, r"мл"),
        (1000, r"л"),
    ]

    Numeric_Concentration = Measure(
        "Numeric Concentration",
        weight=Decimal("0.1"),
        designation="",
        regex=rf"{_num1}\s*{_top}\s*{_sep}\s*{_num2}\s*{_bot}",
    )

    Percent_Concentration = Measure(
        "Percent Concentation",
        weight=Decimal("1"),
        designation="",
        regex=rf"{_num1}\s*%",
    )

    MEASURES: FeatureMeasures = FeatureMeasures(
        measures=[
            Numeric_Concentration,
            Percent_Concentration,
        ],
    )

    NAME = "Concentration"
    VALIDATION_MODE = FeatureValidationMode.MODEST
    NOT_FOUND_MODE = FeatureNotFoundMode.MODEST
    PRIORITY = 2

    def __init__(
        self,
        value: str,
        measure: Measure,
    ) -> None:
        self.original_value = value
        self.standard_weight = 1

        if measure is self.Numeric_Concentration:
            self.standard_value = self._numerical_standartization(value)
        elif measure is self.Percent_Concentration:
            self.standard_value = self._percent_standartization(value)

    def _num_standartization(self, value: str, weights: str) -> Decimal:
        num = re.search(self._num1, value)
        if num:
            num = num[0]
            num = num.replace(",", ".")
        else:
            num = "1"

        num = Decimal(num)

        weight = Decimal("1")
        for _weight in weights:
            if re.search(_weight[1], value):
                weight = _weight[0]
                break

        num = num * Decimal(str(weight))
        return num

    def _numerical_standartization(self, value: str) -> Decimal:
        top, bot = re.split(self._sep, value, re.IGNORECASE)
        top = self._num_standartization(top, self._tops)
        bot = self._num_standartization(bot, self._bots)
        standard = top / bot * self.Numeric_Concentration.weight
        return standard

    def _percent_standartization(self, value: str):
        standard = re.search(self._num1, value)[0]
        standard = standard.replace(",", ".")
        standard = Decimal(standard) * self.Percent_Concentration.weight
        return standard

    def __eq__(self, other: AbstractFeature) -> bool:
        if isinstance(other, self.__class__):
            if self.standard_value == other.standard_value:
                return True
        return False

    def __hash__(self) -> int:
        return hash(self.standard_value)

    def __repr__(self) -> str:
        return f"Concentration = {self.standard_value}"

    def __str__(self) -> str:
        return f"Concentration = {self.standard_value}"

    @classmethod
    @property
    def designations(self):
        return self.MEASURES


class FutureList(object):
    def __init__(
        self,
        future_list: list[AbstractFeature] = [],
    ) -> None:
        if not future_list:
            future_list = self.default_futures()
        self.future_list = self.sort_futures(future_list)

    def sort_futures(self, future_list: list[AbstractFeature]) -> list[AbstractFeature]:
        futures = [(future, future.PRIORITY) for future in future_list]
        futures = sorted(futures, key=lambda future: future[1])
        futures = list(map(lambda future: future[0], futures))
        return futures

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index < len(self.future_list):
            future = self.future_list[self._index]
            self._index += 1
            return future
        else:
            raise StopIteration

    def default_futures(self) -> list[AbstractFeature]:
        return [
            Weight,
            Volume,
            MemoryCapacity,
            Dimension,
            Size,
            Length,
            Quantity,
            Color,
            Concentration,
            ConcentrationPerDose,
            FarmaForm,
        ]


if __name__ == "__main__":
    # lst = FutureList()
    for des in Dimension.designations:
        print(des.regex)
