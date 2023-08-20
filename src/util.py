import pandas as pd

from notation import *


class DataReprMode(object):
    DEFAULT = "default"
    DECART = "decart"


class DataRepr(object):
    """
    Данные могут быть представлены в двух видах:
    1. Обычный вид валидационного файла
    2. Декартово множество на основе названий клиента и названий источников
    """

    def __init__(self, mode: DataReprMode) -> None:
        self.mode = self._checkout_mode(mode)

    def _checkout_mode(self, mode: str) -> str:
        if mode not in [
            DataReprMode.DEFAULT,
            DataReprMode.DECART,
        ]:
            raise NotImplementedError()
        return mode

    def _default_mode(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
    ) -> pd.DataFrame:
        data = validation.merge(
            semantic[[SEMANTIC.NAME, VENDOR_CODE.COLUMN]],
            how="left",
            left_on=VALIDATION.NAME,
            right_on=SEMANTIC.NAME,
        )

        return data.drop(SEMANTIC.NAME, axis=1)

    def _decart_mode(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
    ) -> pd.DataFrame:
        _semantic = semantic.drop_duplicates(subset=[SEMANTIC.NAME])
        _validation = validation.drop_duplicates(
            subset=[
                VALIDATION.SOURCE,
                VALIDATION.VALIDATION_ROW,
            ]
        )[
            [
                VALIDATION.SOURCE,
                VALIDATION.LINK,
                VALIDATION.VALIDATION_ROW,
                VALIDATION.SOURCE_NAME,
            ]
        ]

        data = _validation.merge(_semantic, how="cross")
        # TODO: нужно проверить декартово множество на рациональность
        return data

    def proccess(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
    ) -> pd.DataFrame:
        match self.mode:
            case DataReprMode.DEFAULT:
                return self._default_mode(semantic, validation)
            case DataReprMode.DECART:
                return self._decart_mode(semantic, validation)
