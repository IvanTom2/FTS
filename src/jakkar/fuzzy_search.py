import pandas as pd
from fuzzywuzzy import process as fuzz_process
from tqdm import tqdm
import warnings
import multiprocessing
import numpy as np
import gc


warnings.filterwarnings("ignore")
tqdm.pandas()


from tokenization import Token, TokenTransformer


def old_search_func(
    row: pd.Series,
    left_tokens_column: str,
    right_tokens_column: str,
    transformer: TokenTransformer,
    fuzzy_threshold: int,
):
    left_tokens: list[Token] = row[left_tokens_column]
    right_tokens: list[Token] = row[right_tokens_column]
    right_tokens_values = [token.value for token in right_tokens]

    for left_token in left_tokens:
        if left_token in right_tokens:
            index = right_tokens.index(left_token.value)
            right_token = right_tokens[index]

            transformer.transform(right_token, left_token, False)

        else:
            token_value, score = fuzz_process.extractOne(
                left_token.value,
                right_tokens_values,
            )
            if score >= fuzzy_threshold:
                index = right_tokens.index(token_value)
                right_token = right_tokens[index]

                transformer.transform(right_token, left_token)

    return row


def setup_tasks(
    search_func: callable,
    index: int,
    data: pd.DataFrame,
    left_tokens_column: str,
    right_tokens_column: str,
    transformer: TokenTransformer,
    fuzzy_threshold: int,
) -> pd.DataFrame:
    if not index:  # that mean index == 0
        data = data.progress_apply(
            search_func,
            args=(
                left_tokens_column,
                right_tokens_column,
                transformer,
                fuzzy_threshold,
            ),
            axis=1,
        )

    else:
        data = data.apply(
            search_func,
            args=(
                left_tokens_column,
                right_tokens_column,
                transformer,
                fuzzy_threshold,
            ),
            axis=1,
        )

    return data


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

    def _old_search(
        self,
        row: pd.Series,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> pd.Series:
        return old_search_func(
            row,
            left_tokens_column,
            right_tokens_column,
            self.transformer,
            self.fuzzy_threshold,
        )

    def _slice(
        self,
        data: pd.DataFrame,
        process_pool: multiprocessing.Pool,
    ) -> list[pd.DataFrame]:
        return np.array_split(data, process_pool._processes)

    def old_search(
        self,
        data: pd.DataFrame,
        left_tokens_column: str,
        right_tokens_column: str,
        process_pool: multiprocessing.Pool,
    ) -> pd.DataFrame:
        if process_pool:
            dataframes = self._slice(data, process_pool)
            args = [
                (
                    old_search_func,
                    index,
                    dataframes[index],
                    left_tokens_column,
                    right_tokens_column,
                    self.transformer,
                    self.fuzzy_threshold,
                )
                for index in range(len(dataframes))
            ]

            results = []
            for result in process_pool.starmap(setup_tasks, args):
                results.append(result)

            data = pd.concat(results)

        else:
            data = data.progress_apply(
                self._old_search,
                args=(
                    left_tokens_column,
                    right_tokens_column,
                ),
                axis=1,
            )

        return data

    def _fuzz_extract(
        self,
        left_token: Token,
        right_tokens_values: list[str],
    ) -> tuple[int, Token]:
        try:
            token_value, score = left_token.value, 0
            extracted = fuzz_process.extractOne(
                left_token.value,
                right_tokens_values,
            )

            if extracted:
                token_value, score = extracted

        except Exception as ex:
            print(ex)
            token_value, score = left_token.value, 0

        finally:
            return token_value, score

    def _search_func(
        self,
        row: list[Token],
    ) -> tuple[list[Token]]:
        left_tokens: list[Token] = row[0]
        right_tokens: list[Token] = row[1]
        right_tokens_values = [token.value for token in right_tokens]

        for left_token in left_tokens:
            if left_token in right_tokens:
                index = right_tokens.index(left_token.value)
                right_token = right_tokens[index]

                self.transformer.transform(right_token, left_token, False)

            else:
                token_value, score = self._fuzz_extract(left_token, right_tokens_values)
                if score >= self.fuzzy_threshold:
                    index = right_tokens.index(token_value)
                    right_token = right_tokens[index]

                    self.transformer.transform(right_token, left_token, True)

        return left_tokens, right_tokens

    def search(
        self,
        data: pd.DataFrame,
        left_tokens_column: str,
        right_tokens_column: str,
        process_pool: multiprocessing.Pool,
    ) -> pd.DataFrame:
        left_tokens = data[left_tokens_column].to_list()
        right_tokens = data[right_tokens_column].to_list()

        massive = list(zip(left_tokens, right_tokens))
        massive = list(map(self._search_func, tqdm(massive)))

        data[left_tokens_column, right_tokens_column] = massive
        return data
