import pandas as pd
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "src"))

from src.util import DataRepr, DataReprMode
from src.vendor_code import VendorCodeSearch, VendorCodeExtractor
from src.text_feature import TextFeatureSearch
from notation import *
from collections.abc import Callable
from jakkar.jakkar import *


class Validator(object):
    def __init__(
        self,
        data_repr: DataRepr,
        VCExtractor: VendorCodeExtractor,
        VC: VendorCodeSearch,
        TF: TextFeatureSearch,
        jakkar: FuzzyJakkarValidator,
    ) -> None:
        self.VC = VC
        self.VCExtractor = VCExtractor
        self.TF = TF
        self.jakkar = jakkar
        self.data_repr = data_repr

    def _upload_data(self, path: str) -> pd.DataFrame:
        return pd.read_excel(path)

    def _get_data(
        self,
        semantic_path: str,
        validation_path: str,
        data_repr: DataRepr,
    ):
        semantic = self._upload_data(semantic_path)
        semantic = self.VCExtractor.extract(semantic)

        validation = self._upload_data(validation_path)
        data = data_repr.proccess(semantic, validation)

        data[VALIDATED] = 0
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
        validation_path: str,
    ) -> pd.DataFrame:
        data = self._get_data(
            semantic_path,
            validation_path,
            self.data_repr,
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
    VC = VendorCodeSearch(True)
    TF = TextFeatureSearch(True)
    VCExtractor = VendorCodeExtractor()

    jakkar = FuzzyJakkarValidator(
        tokenizer=BasicTokenizer(),
        preprocessor=Preprocessor(0),
        fuzzy=FuzzySearch(65, transformer=TokenTransformer()),
        rate_counter=RateCounter(0, 1, 1, 0, RateFunction.sqrt2),
        marks_counter=MarksCounter(MarksMode.UNION),
        validation_treshold=50,
        debug=True,
    )

    validator = Validator(
        data_repr=data_repr,
        VC=VC,
        TF=TF,
        VCExtractor=VCExtractor,
        jakkar=jakkar,
    )

    validator.validate(
        semantic_path="semantic.xlsx",
        validation_path="validation.xlsx",
    )
