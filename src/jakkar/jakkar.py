import pandas as pd
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from notation import *
from tokenization import BasicTokenizer, TokenTransformer, Token
from preprocessing import Preprocessor
from fuzzy_search import FuzzySearch
from ratio import RateCounter, MarksCounter, MarksMode


CLIENT = "_client"
SOURCE = "_source"
CLIENT_TOKENS = "_client_tokens"
SOURCE_TOKENS = "_source_tokens"


class FuzzyJakkarValidator(object):
    def __init__(
        self,
        tokenizer: BasicTokenizer,
        preprocessor: Preprocessor,
        fuzzy: FuzzySearch,
        rate_counter: RateCounter,
        marks_counter: MarksCounter,
        debug: bool = False,
    ) -> None:
        self.tokenizer = tokenizer
        self.preproc = preprocessor
        self.fuzzy = fuzzy
        self.rate_counter = rate_counter
        self.marks_counter = marks_counter
        self.debug = debug

        self.symbols_to_del = r"'\"/"

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

    def _create_working_rows(self, validation: pd.DataFrame) -> pd.DataFrame:
        validation[CLIENT] = self._delete_symbols(validation[VALIDATION_CLIENT_NAME])
        validation[SOURCE] = self._delete_symbols(validation[VALIDATION_ROW])
        return validation

    def _delete_working_rows(self, validation: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("end")
        validation.drop([CLIENT, SOURCE], axis=1, inplace=True)
        if not self.debug:
            validation.drop([CLIENT_TOKENS, SOURCE_TOKENS], axis=1, inplace=True)
        return validation

    def _save_ratio(self) -> None:
        file = pd.Series(data=self.ratio)
        file.to_excel("ratio.xlsx")

    def _process_tokenization(self, data: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("client_tokens")
        data = self.tokenizer.tokenize(data, CLIENT, CLIENT_TOKENS)

        self._progress_ind("source_tokens")
        data = self.tokenizer.tokenize(data, SOURCE, SOURCE_TOKENS)

        return data

    def _make_tokens_set(self, data: pd.DataFrame) -> pd.DataFrame:
        data[CLIENT_TOKENS] = data[CLIENT_TOKENS].apply(set)
        data[SOURCE_TOKENS] = data[SOURCE_TOKENS].apply(set)
        return data

    def _process_preprocessing(self, validation: pd.DataFrame) -> pd.DataFrame:
        validation[CLIENT_TOKENS] = self.preproc.preprocess(validation[CLIENT_TOKENS])
        validation[SOURCE_TOKENS] = self.preproc.preprocess(validation[SOURCE_TOKENS])
        return validation

    def _process_fuzzy(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self.fuzzy.search(data, CLIENT_TOKENS, SOURCE_TOKENS)
        return data

    def _process_ratio(self, data: pd.DataFrame) -> pd.DataFrame:
        ratio = self.rate_counter.count_ratio(data, CLIENT_TOKENS, SOURCE_TOKENS)
        return ratio

    def _process_marks_count(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.marks_counter.count_marks(
            self.ratio,
            data,
            CLIENT_TOKENS,
            SOURCE_TOKENS,
        )

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self._create_working_rows(data)

        data = self._process_tokenization(data)
        data = self._process_preprocessing(data)

        data = self._process_fuzzy(data)
        self.ratio = self._process_ratio(data)

        data = self._make_tokens_set(data)
        data = self._process_marks_count(data)

        if self.debug:
            self._save_ratio()

        data = self._delete_working_rows(data)
        return data


if __name__ == "__main__":
    data = pd.read_excel(r"/home/mainus/BrandPol/src/jakkar/test.xlsx")

    tokenizer = BasicTokenizer()
    preprocessor = Preprocessor()
    transformer = TokenTransformer()
    rate_counter = RateCounter()
    fuzzy = FuzzySearch(65, transformer=transformer)
    marks_counter = MarksCounter(MarksMode.UNION)

    validator = FuzzyJakkarValidator(
        tokenizer=tokenizer,
        preprocessor=preprocessor,
        fuzzy=fuzzy,
        rate_counter=rate_counter,
        marks_counter=marks_counter,
        debug=True,
    )

    validator.validate(data)
