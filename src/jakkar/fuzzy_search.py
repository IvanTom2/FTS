import pandas as pd
from fuzzywuzzy import process as fuzz_process
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")
tqdm.pandas()

from tokenization import Token, TokenTransformer


class FuzzySearch(object):
    def __init__(
        self,
        fuzzy_threshold: int,
        transformer: TokenTransformer,
    ) -> None:
        if fuzzy_threshold > 100 or fuzzy_threshold < 0:
            raise ValueError("Fuzzy threshold should be in range 0 to 100")
        self.fuzzy_threshold = fuzzy_threshold
        self.transformer = transformer

    def _search(
        self,
        row: pd.Series,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> pd.Series:
        left_tokens: list[Token] = row[left_tokens_column]
        right_tokens: list[Token] = row[right_tokens_column]
        right_tokens_values = [token.value for token in right_tokens]

        for left_token in left_tokens:
            if left_token in right_tokens:
                index = right_tokens.index(left_token.value)
                right_token = right_tokens[index]

                self.transformer.transform(right_token, left_token, False)

            else:
                token_value, score = fuzz_process.extractOne(
                    left_token.value,
                    right_tokens_values,
                )
                if score >= self.fuzzy_threshold:
                    index = right_tokens.index(token_value)
                    right_token = right_tokens[index]

                    self.transformer.transform(right_token, left_token)

        return row

    def search(
        self,
        data: pd.DataFrame,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> pd.DataFrame:
        data = data.progress_apply(
            self._search,
            args=(
                left_tokens_column,
                right_tokens_column,
            ),
            axis=1,
        )
        return data
