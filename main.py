import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(__file__) + "/src")
sys.path.append(os.path.dirname(__file__) + "\src")

from src.util import Mode
from src.vendor_code import VendorCodeSearch, VendorCodeExtractor
from src.text_feature import AbstractTextFeatureSearch, TextFeatureSearch
from notation import *
from collections.abc import Callable


class Validator(object):
    def __init__(
        self,
        VC: VendorCodeSearch,
        TF: AbstractTextFeatureSearch,
        VCExtractor: VendorCodeExtractor,
    ) -> None:
        self.VC = VC
        self.TF = TF
        self.VCExtractor = VCExtractor

    def _upload_data(self, path: str) -> pd.DataFrame:
        return pd.read_excel(path)

    def _get_data(
        self,
        semantic_path: str,
        validation_path: str,
        mode: Mode,
    ):
        semantic = self._upload_data(semantic_path)
        semantic = self.VCExtractor.extract(semantic)

        validation = self._upload_data(validation_path)
        data = mode.proccess(semantic, validation)

        data[VALIDATED] = 0
        return data

    def _proccess_validation(
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
        mode: Mode,
    ) -> pd.DataFrame:
        data = self._get_data(semantic_path, validation_path, mode)

        data = self._proccess_validation(self.VC.validate, data)
        data = self._proccess_validation(self.TF.validate, data)

        print(data)


if __name__ == "__main__":
    """
    1. Вызов семантики и валидационного файла
    2. Выбор режима - таблица или Декартово множество
    3. Предобработка строк
    4. Запуск поиска по артикулу
    5. Запуск поиска по текстовым признакам
    6. Запуск Джаккара
    """

    mode = Mode("default")
    VC = VendorCodeSearch(True)
    TF = TextFeatureSearch(True)
    VCExtractor = VendorCodeExtractor()

    validator = Validator(VC=VC, TF=TF, VCExtractor=VCExtractor)
    validator.validate(
        semantic_path="semantic.xlsx",
        validation_path="validation.xlsx",
        mode=mode,
    )
