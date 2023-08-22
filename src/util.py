import pandas as pd

from notation import RAW, SEMANTIC, DATA


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

    def _rename(self, data: pd.DataFrame):
        return data.rename(DATA.rename)

    def extract_domain(self, raw: pd.DataFrame) -> pd.DataFrame:
        rx = "://(?:.*\.)?([a-zа-яё]+\.[a-zа-яё]+)"
        raw[RAW.SOURCE] = raw[RAW.LINK].str.extract(rx)
        return raw

    def _default_mode(
        self,
        semantic: pd.DataFrame,
        raw: pd.DataFrame,
    ) -> pd.DataFrame:
        data = raw[DATA.raw_cols].merge(
            semantic[DATA.sem_cols],
            how="left",
            left_on=RAW.QUERY,
            right_on=SEMANTIC.QUERY,
        )

        return self._change_columns(data)

    def _decart_mode(
        self,
        semantic: pd.DataFrame,
        raw: pd.DataFrame,
    ) -> pd.DataFrame:
        _semantic = semantic.drop_duplicates(
            subset=[
                SEMANTIC.NAME,
                SEMANTIC.QUERY,
            ],
        )[DATA.sem_cols]

        _validation = raw.drop_duplicates(
            subset=[
                RAW.SOURCE,
                RAW.ROW,
            ]
        )[DATA.raw_cols]

        data = _validation.merge(_semantic, how="cross")
        # TODO: нужно проверить декартово множество на рациональность
        return self._change_columns(data)

    def _change_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.drop(DATA.to_drop, axis=1)
        data = data.rename(columns=DATA.rename)
        data = data[DATA.columns_order]
        return data

    def proccess(
        self,
        semantic: pd.DataFrame,
        raw: pd.DataFrame,
    ) -> pd.DataFrame:
        raw = self.extract_domain(raw)

        match self.mode:
            case DataReprMode.DEFAULT:
                return self._default_mode(semantic, raw)
            case DataReprMode.DECART:
                return self._decart_mode(semantic, raw)


class TestDataPreprocess(object):
    pass
