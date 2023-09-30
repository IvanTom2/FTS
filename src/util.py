import pandas as pd
import re


from notation import RAW, SEMANTIC, DATA, PATH
from vendor_code import VendorCodeSearch, VendorCodeExtractor


def extract_domain(raw: pd.DataFrame) -> pd.DataFrame:
    rx = "://(?:.*\.)?([a-zа-яё0-9_-]+\.[a-zа-яё]+)/"
    raw[RAW.SOURCE] = raw[RAW.LINK].str.extract(rx)
    return raw


class DataReprMode(object):
    VALIDATION = "validation"
    RAW = "raw"
    RAW_DECART = "decart"


class DataRepr(object):
    """
    Данные могут быть представлены в двух видах:
    1. Обычный вид валидационного файла
    2. Сырые данные и семантика, превращенные валидационный файл
    3. Декартово множество
    """

    def __init__(
        self,
        mode: DataReprMode,
        # VCExtractor: VendorCodeExtractor,
    ) -> None:
        self.mode = self._checkout_mode(mode)
        # self.VCExtractor = VCExtractor

    def _checkout_mode(self, mode: str) -> str:
        if mode not in [
            DataReprMode.RAW,
            DataReprMode.RAW_DECART,
            DataReprMode.VALIDATION,
        ]:
            raise NotImplementedError()
        return mode

    def _rename(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.rename(DATA.rename)

    def _expand_str_column(self, data: pd.DataFrame, column: str) -> pd.DataFrame:
        data[column] = " " + data[column] + " "
        return data

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

        if RAW.MYMARK in raw.columns:
            data[DATA.MYMARK] = raw[RAW.MYMARK]

        return self._change_columns(data)

    def _extract_domain(self, data: pd.DataFrame) -> pd.DataFrame:
        return extract_domain(data)

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

        not_in_order = [col for col in data.columns if col not in DATA.columns_order]
        data = data[DATA.columns_order + not_in_order]
        return data

    def _upload_data(self, path: str) -> pd.DataFrame:
        return pd.read_excel(path)

    def _process_raw(
        self,
        semantic_path: str,
        raw_path: str,
    ) -> pd.DataFrame:
        semantic = self._upload_data(semantic_path)
        raw = self._upload_data(raw_path)
        raw = self._extract_domain(raw)

        if self.mode is DataReprMode.RAW:
            data = self._default_mode(semantic, raw)
        elif self.mode is DataReprMode.RAW_DECART:
            data = self._decart_mode(semantic, raw)

        return data

    def _process_validation(
        self,
        validation_path: str,
    ) -> pd.DataFrame:
        data = self._upload_data(validation_path)
        return data

    def proccess(
        self,
        semantic_path: str,
        raw_path: str,
        validation_path: str,
    ) -> pd.DataFrame:
        match self.mode:
            case DataReprMode.RAW:
                data = self._process_raw(semantic_path, raw_path)
            case DataReprMode.RAW_DECART:
                data = self._process_raw(semantic_path, raw_path)
            case DataReprMode.VALIDATION:
                data = self._process_validation(validation_path)

        # if self.VCExtractor is not None:
        #     data = self.VCExtractor.extract(data)

        data = self._expand_str_column(data, DATA.ROW)
        data = self._expand_str_column(data, DATA.CLIENT_NAME)
        return data


class TestDataPreprocess(object):
    def __init__(self):
        pass

    def _regex_check(self, row: pd.Series) -> pd.Series:
        mark = re.search(
            row[RAW.FAST_CHECK],
            row[RAW.ROW],
            flags=re.IGNORECASE,
        )
        row[RAW.MARK] = 1 if mark else 0
        return row

    def _fast_check(self, semantic: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
        raw = raw.merge(
            semantic[[SEMANTIC.QUERY, SEMANTIC.PLUS, SEMANTIC.CLIENT_NAME]],
            how="left",
            left_on=RAW.QUERY,
            right_on=SEMANTIC.QUERY,
        )

        raw = raw.drop([SEMANTIC.QUERY], axis=1)
        raw = raw.rename(columns={SEMANTIC.PLUS: RAW.FAST_CHECK})
        raw = raw.apply(self._regex_check, axis=1)
        return raw

    def preprocess(self, semantic: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
        raw = self._fast_check(semantic, raw)

        raw = extract_domain(raw)
        raw = raw.drop_duplicates(subset=[RAW.ROW, RAW.LINK])
        raw = raw.sort_values(by=RAW.MARK, ascending=False)

        raw = raw.groupby(
            [RAW.SOURCE, RAW.QUERY],
            group_keys=True,
        ).apply(lambda x: x[:10])
        return raw


if __name__ == "__main__":
    prepr = TestDataPreprocess()

    semantic = pd.read_excel(PATH.Technique.file_path("semantic"))
    raw = pd.read_excel(PATH.Technique.file_path("raw_original"))

    raw = prepr.preprocess(semantic, raw)
    raw.to_excel("test.xlsx", index=False)
