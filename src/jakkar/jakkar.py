import sys
import pandas as pd
import numpy as np
from pathlib import Path
import multiprocessing


sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent / "jakkar" / "strmod"))


from notation import JAKKAR, DATA
from preprocessing import Preprocessor
from fuzzy_search import FuzzySearch
from ratio import RateCounter, MarksCounter, MarksMode, RateFunction
from tokenization import (
    BasicTokenizer,
    TokenTransformer,
    RegexTokenizer,
    RegexCustomWeights,
    LanguageType,
)
from main_util import TEST_DATA


class FuzzyJakkarValidator(object):
    def __init__(
        self,
        tokenizer: BasicTokenizer,
        preprocessor: Preprocessor,
        fuzzy: FuzzySearch,
        rate_counter: RateCounter,
        marks_counter: MarksCounter,
        debug: bool = False,
        validation_treshold: float = 0.5,
    ) -> None:
        if validation_treshold < 0 or validation_treshold > 1:
            raise ValueError("Validation treshold should be in range 0 - 1")

        self.tokenizer = tokenizer
        self.preproc = preprocessor
        self.fuzzy = fuzzy
        self.rate_counter = rate_counter
        self.marks_counter = marks_counter
        self.debug = debug
        self.validation_treshold = validation_treshold

        self.symbols_to_del = r"'\"/"
        self.returning_columns = [
            JAKKAR.CLIENT_TOKENS_COUNT,
            JAKKAR.SOURCE_TOKENS_COUNT,
            JAKKAR.VALIDATED,
            DATA.VALIDATED,
        ]

        if self.debug:
            self.returning_columns.extend(
                [
                    JAKKAR.CLIENT_TOKENS,
                    JAKKAR.SOURCE_TOKENS,
                ]
            )

    def _progress_ind(self, indicator: str) -> None:
        match indicator:
            case "start":
                print("Start Fuzzy Jakkar matching")
            case "client_tokens":
                print("Tokenize client tokens")
            case "source_tokens":
                print("Tokenize site tokens")
            case "make_filter":
                print("Make tokens filter")
            case "make_set":
                print("Make set of tokens")
            case "make_fuzzy":
                print("Start fuzzy search")
            case "make_ratio":
                print("Start count match ratio")
            case "end":
                print("End the validation process: save output")
            case "delete_rx":
                print("Deleting elements from rows by regex")

    def _delete_symbols(self, series: pd.Series):
        symbols_to_del = "|".join(list(self.symbols_to_del))
        series = series.str.replace(symbols_to_del, "", regex=True)
        return series

    def _create_working_rows(self, data: pd.DataFrame) -> pd.DataFrame:
        data[JAKKAR.CLIENT] = self._delete_symbols(data[DATA.CLIENT_NAME])
        data[JAKKAR.SOURCE] = self._delete_symbols(data[DATA.ROW])
        return data

    def _delete_working_rows(self, data: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("end")
        data.drop(
            [JAKKAR.CLIENT, JAKKAR.SOURCE],
            axis=1,
            inplace=True,
        )

        if not self.debug:
            data.drop(
                [JAKKAR.CLIENT_TOKENS, JAKKAR.SOURCE_TOKENS],
                axis=1,
                inplace=True,
            )
        return data

    def _save_ratio(self) -> None:
        pd.Series(data=self.ratio).to_excel(JAKKAR.RATIO_PATH)

    def _process_tokenization(self, data: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("client_tokens")
        data = self.tokenizer.tokenize(data, JAKKAR.CLIENT, JAKKAR.CLIENT_TOKENS)

        self._progress_ind("source_tokens")
        data = self.tokenizer.tokenize(data, JAKKAR.SOURCE, JAKKAR.SOURCE_TOKENS)

        return data

    def _make_tokens_set(self, data: pd.DataFrame) -> pd.DataFrame:
        data[JAKKAR.CLIENT_TOKENS] = data[JAKKAR.CLIENT_TOKENS].apply(set)
        data[JAKKAR.SOURCE_TOKENS] = data[JAKKAR.SOURCE_TOKENS].apply(set)
        return data

    def _process_preprocessing(self, validation: pd.DataFrame) -> pd.DataFrame:
        validation[JAKKAR.CLIENT_TOKENS] = self.preproc.preprocess(
            validation[JAKKAR.CLIENT_TOKENS]
        )
        validation[JAKKAR.SOURCE_TOKENS] = self.preproc.preprocess(
            validation[JAKKAR.SOURCE_TOKENS]
        )
        return validation

    def _process_fuzzy(
        self,
        data: pd.DataFrame,
        process_pool: multiprocessing.Pool,
    ) -> pd.DataFrame:
        self._progress_ind("make_fuzzy")
        data = self.fuzzy.search(
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
            process_pool,
        )
        return data

    def _process_ratio(self, data: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("make_ratio")
        ratio = self.rate_counter.count_ratio(
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
        )
        return ratio

    def _process_marks_count(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        return self.marks_counter.count_marks(
            self.ratio,
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
            self.returning_columns,
        )

    def _process_tokens_count(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        data[JAKKAR.CLIENT_TOKENS_COUNT] = data[JAKKAR.CLIENT_TOKENS].apply(
            lambda x: len(x)
        )
        data[JAKKAR.SOURCE_TOKENS_COUNT] = data[JAKKAR.SOURCE_TOKENS].apply(
            lambda x: len(x)
        )
        return data

    def validate(
        self,
        data: pd.DataFrame,
        process_pool: multiprocessing.Pool,
    ) -> tuple[pd.DataFrame, list[str]]:
        data = self._create_working_rows(data)

        data = self._process_tokenization(data)
        data = self._process_preprocessing(data)

        # очистка токенов-символов по типу (, ), \, . и т.д.
        # актуально для word_tokenizer
        data = self._process_fuzzy(data, process_pool)
        self.ratio = self._process_ratio(data)

        data = self._make_tokens_set(data)
        data = self._process_tokens_count(data)
        data = self._process_marks_count(data)

        if self.debug:
            self._save_ratio()

        data = self._delete_working_rows(data)

        data[JAKKAR.VALIDATED] = np.where(
            data[self.marks_counter.validation_column] >= self.validation_treshold,
            data[JAKKAR.VALIDATED],
            0,
        )

        data[DATA.VALIDATED] = np.where(
            data[JAKKAR.VALIDATED] == 1,
            data[DATA.VALIDATED],
            0,
        )

        return data, self.returning_columns


if __name__ == "__main__":
    data = pd.read_excel(TEST_DATA.VKUSVILL2)

    regex_weights = RegexCustomWeights(1, 1, 1, 1)
    tokenizer = RegexTokenizer(
        {
            LanguageType.RUS: 1,
            LanguageType.ENG: 1,
        },
        weights_rules=regex_weights,
    )

    preprocessor = Preprocessor(2)
    transformer = TokenTransformer()
    rate_counter = RateCounter(0.1, 0.2, 2, 0, RateFunction.sqrt2)
    fuzzy = FuzzySearch(75, transformer=transformer)
    marks_counter = MarksCounter(MarksMode.MULTIPLE)

    validator = FuzzyJakkarValidator(
        tokenizer=tokenizer,
        preprocessor=preprocessor,
        fuzzy=fuzzy,
        rate_counter=rate_counter,
        marks_counter=marks_counter,
        debug=True,
    )

    # data, cols = validator.validate(data)
    # data.to_excel("vkusvill2_checkout.xlsx")
