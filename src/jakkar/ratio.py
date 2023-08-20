from abc import ABC, abstractmethod
import pandas as pd
from typing import Callable

from tokenization import Token

from collections import Counter
from itertools import chain
from math import sqrt


class AbstactRateCounter(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def count_ratio(self):
        pass


class RateFunction(object):
    @classmethod
    def default(self, value: int) -> int:
        return value

    @classmethod
    def sqrt2(self, value: int) -> float:
        if value != 0:
            return sqrt(value)
        return value


class RateCounter(AbstactRateCounter):
    def __init__(
        self,
        min_ratio: float = 0,
        max_ratio: float = 1,
        uniq_max_value: int = 1,
        uniq_penalty: float = 0,
        rate_function: Callable = RateFunction.default,
    ) -> None:
        self.min_ratio = min_ratio
        self.max_ratio = max_ratio
        self.uniq_max_value = uniq_max_value
        self.uniq_penalty = uniq_penalty
        self.rate_function = rate_function

    def _get_tokens(
        self,
        data: pd.DataFrame,
        left_tokens: str,
        right_tokens: str,
    ) -> list[Token]:
        tokens: list[list[Token]] = (
            data[left_tokens].to_list() + data[right_tokens].to_list()
        )
        tokens: list[Token] = list(chain(*tokens))

        # in case if passed sets of tokens
        # tokens: list[Token] = list(chain(*map(list, tokens)))
        return tokens

    def _rate_function(self, value: int) -> float:
        try:
            if self.rate_function is not None:
                if callable(self.rate_function):
                    value = self.rate_function(value)

            if value != 0:
                value_rate = 1 / value
            else:
                value_rate = 0

        except Exception as ex:
            print(ex)

        finally:
            return value_rate

    def _uniq_penalty(self, value: int) -> float:
        if self.uniq_max_value:
            if value <= self.uniq_max_value:
                return self.uniq_penalty
        return 1

    def _min_max(self, value_rate: float) -> float:
        value_rate = self.min_ratio if value_rate < self.min_ratio else value_rate
        value_rate = self.max_ratio if value_rate > self.max_ratio else value_rate
        return value_rate

    def _count_ratio(self, value: int) -> float:
        value_rate = self._rate_function(value)
        value_rate = self._min_max(value_rate)
        value_rate *= self._uniq_penalty(value)

        return value_rate

    def _process_ratio(self, tokens: list[Token]):
        ratio = {}
        tokens_values = [token.value for token in tokens]
        counts = Counter(tokens_values)

        for key, value in counts.items():
            rate = self._count_ratio(value)
            ratio[key] = rate

        return ratio

    def count_ratio(
        self,
        data: pd.DataFrame,
        left_tokens: str,
        right_tokens: str,
    ) -> dict:
        tokens = self._get_tokens(data, left_tokens, right_tokens)
        ratio = self._process_ratio(tokens)
        return ratio


class AbstractMarksCounter(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def count_marks(self):
        pass


class MarksMode(object):
    UNION = "marks_union"
    CLIENT = "marks_client"
    SOURCE = "marks_source"
    MULTIPLE = "multiple"


class MarksCounter(AbstractMarksCounter):
    def __init__(
        self,
        mode: MarksMode,
    ) -> None:
        self.mode = mode

    def _find_ratio(self, tokens: set[Token]):
        tokens_rates = [
            self.ratio[token.value] * token.custom_weight for token in tokens
        ]
        return tokens_rates

    def _try_count_mark(
        self,
        intersect_rates: list[float],
        base_rates: list[float],
    ) -> float:
        try:
            mark = sum(intersect_rates) / sum(base_rates)

        except Exception as ex:
            print(ex)
            return 0

        finally:
            return mark

    def _count_mark(
        self,
        row: pd.Series,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> float:
        left_tokens: set[Token] = row[left_tokens_column]
        right_tokens: set[Token] = row[right_tokens_column]
        intersect = left_tokens.intersection(right_tokens)

        if self.mode is MarksMode.UNION:
            base = left_tokens.union(right_tokens)
        elif self.mode == MarksMode.CLIENT:
            base = left_tokens
        elif self.mode == MarksMode.SOURCE:
            base = right_tokens
        else:
            raise NotImplementedError("Not implemented Marks Mode")

        intersect_rates = self._find_ratio(intersect)
        base_rates = self._find_ratio(base)

        mark = self._try_count_mark(intersect_rates, base_rates)
        return mark

    def _count_multiple_marks(
        self,
        row: pd.Series,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> list[float]:
        left_tokens: set[Token] = row[left_tokens_column]
        right_tokens: set[Token] = row[right_tokens_column]

        intersect = left_tokens.intersection(right_tokens)

        bases = [
            left_tokens.union(right_tokens),
            left_tokens,
            right_tokens,
        ]

        intersect_rates = self._find_ratio(intersect)
        marks = [
            self._try_count_mark(intersect_rates, self._find_ratio(base))
            for base in bases
        ]
        return marks

    def count_marks(
        self,
        ratio: dict,
        data: pd.DataFrame,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> pd.Series:
        self.ratio = ratio

        if self.mode is MarksMode.MULTIPLE:
            marks = data.apply(
                self._count_multiple_marks,
                axis=1,
                args=(left_tokens_column, right_tokens_column),
            )

            data[MarksMode.UNION] = marks.str[0]
            data[MarksMode.CLIENT] = marks.str[1]
            data[MarksMode.SOURCE] = marks.str[2]

        else:
            data[self.mode] = data.apply(
                self._count_mark,
                axis=1,
                args=(left_tokens_column, right_tokens_column),
            )

        return data


if __name__ == "__main__":
    val = 0
    print(RateFunction.sqrt2(val))
