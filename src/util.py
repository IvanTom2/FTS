import pandas as pd

from notation import *


class Mode(object):
    def __init__(self, mode: str) -> None:
        self.mode = self._checkout_mode(mode)

    def _checkout_mode(self, mode: str) -> str:
        assert mode in ["default", "decart"], "Mode should be default or decart"
        return mode

    def _default_mode(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
    ) -> pd.DataFrame:
        data = validation.merge(
            semantic[[SEMANTIC_NAME, VENDOR_CODE]],
            how="left",
            left_on=VALIDATION_NAME,
            right_on=SEMANTIC_NAME,
        )

        return data.drop(SEMANTIC_NAME, axis=1)

    def _decart_mode(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
    ) -> pd.DataFrame:
        _semantic = semantic.drop_duplicates(subset=[SEMANTIC_NAME])
        _validation = validation.drop_duplicates(
            subset=[VALIDATION_SOURCE, VALIDATION_ROW]
        )[[VALIDATION_SOURCE, VALIDATION_LINK, VALIDATION_ROW, VALIDATION_SOURCE_NAME]]

        data = _validation.merge(_semantic, how="cross")
        # TODO: нужно проверить декартово множество на релевантность
        return data

    def proccess(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
    ) -> pd.DataFrame:
        match self.mode:
            case "default":
                return self._default_mode(semantic, validation)
            case "decart":
                return self._decart_mode(semantic, validation)
