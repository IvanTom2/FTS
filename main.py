import pandas as pd
import os
import sys
from pathlib import Path
from typing import Union

sys.path.append(str(Path(__file__).parent / "src"))

from src.util import DataRepr, DataReprMode
from src.vendor_code import VendorCodeSearch, VendorCodeExtractor
from src.text_feature import TextFeatureSearch
from notation import DATA, VENDOR_CODE, FEATURES
from collections.abc import Callable
from jakkar.jakkar import *


class Validator(object):
    def __init__(
        self,
        data_repr: DataRepr,
        VCExtractor: VendorCodeExtractor,
        VC: Union[VendorCodeSearch, None],
        TF: Union[TextFeatureSearch, None],
        jakkar: Union[FuzzyJakkarValidator, None],
    ) -> None:
        self.data_repr = data_repr
        self.VCExtractor = VCExtractor
        self.VC = VC
        self.TF = TF
        self.jakkar = jakkar

    def _upload_data(self, path: str) -> pd.DataFrame:
        return pd.read_excel(path)

    def _get_data(
        self,
        semantic_path: str,
        raw_path: str,
    ):
        semantic = self._upload_data(semantic_path)
        if self.VC is not None:
            semantic = self.VCExtractor.extract(semantic)

        raw = self._upload_data(raw_path)
        data = self.data_repr.proccess(semantic, raw)

        data[DATA.VALIDATED] = 0
        data[VENDOR_CODE.VALIDATED] = 0
        data[FEATURES.VALIDATED] = 0
        return data

    def _process_validation(
        self,
        function: Callable,
        data: pd.DataFrame,
    ):
        new_data: pd.DataFrame
        new_data, columns = function(data)

        data.loc[new_data.index, columns] = new_data[columns]
        return data

    def validate(
        self,
        semantic_path: str,
        raw_path: str,
    ) -> pd.DataFrame:
        data = self._get_data(
            semantic_path,
            raw_path,
        )

        if self.VC is not None:
            data = self._process_validation(self.VC.validate, data)

        if self.TF is not None:
            data = self._process_validation(self.TF.validate, data)

        if self.jakkar is not None:
            data = self._process_validation(self.jakkar.validate, data)

        return data


if __name__ == "__main__":
    """
    1. Вызов семантики и валидационного файла
    2. Выбор режима - таблица или Декартово множество
    3. Предобработка строк
    4. Запуск поиска по артикулу
    5. Запуск поиска по текстовым признакам
    6. Запуск Джаккара
    """

    data_repr = DataRepr(DataReprMode.DEFAULT)
    VCExtractor = VendorCodeExtractor()
    VC = VendorCodeSearch(True)
    TF = TextFeatureSearch(True)

    jakkar = FuzzyJakkarValidator(
        tokenizer=BasicTokenizer(),
        preprocessor=Preprocessor(2),
        fuzzy=FuzzySearch(65, transformer=TokenTransformer()),
        rate_counter=RateCounter(0, 1, 1, 0, RateFunction.sqrt2),
        marks_counter=MarksCounter(MarksMode.MULTIPLE),
        validation_treshold=50,
        debug=True,
    )

    validator = Validator(
        data_repr=data_repr,
        VCExtractor=VCExtractor,
        # VC=VC,
        VC=None,
        TF=TF,
        jakkar=jakkar,
    )

    result = validator.validate(
        semantic_path="test_semantic.xlsx",
        raw_path="test_raw.xlsx",
    )

    result.to_excel("output.xlsx", index=False)
