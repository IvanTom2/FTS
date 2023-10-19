import os
import sys
import time
import pandas as pd
import multiprocessing
from pathlib import Path
from typing import Union
from collections.abc import Callable

sys.path.append(str(Path(__file__).parent / "src"))

from src.util import DataRepr, DataReprMode
from src.vendor_code import VendorCodeSearch, VendorCodeExtractor
from src.text_feature import TextFeatureSearch
from src.metrics import Metric, JakkarMetric
from notation import DATA, VENDOR_CODE, FEATURES, JAKKAR
from main_util import TEST_DATA
from jakkar.jakkar import (
    FuzzyJakkarValidator,
    RegexTokenizer,
    LanguageType,
    RegexCustomWeights,
    FuzzySearch,
    TokenTransformer,
    Preprocessor,
    RateCounter,
    RateFunction,
    MarksCounter,
    MarksMode,
)
from src.features_collection import (
    Weight,
    Volume,
    MemoryCapacity,
    Dimension,
    Size,
    Length,
    Quantity,
    Color,
    Concentration,
    ConcentrationPerDose,
    FarmaForm,
)


class Validator(object):
    def __init__(
        self,
        data_repr: DataRepr,
        VC: Union[VendorCodeSearch, None],
        TF: Union[TextFeatureSearch, None],
        jakkar: Union[FuzzyJakkarValidator, None],
        max_processes: int = 0,
    ) -> None:
        self.data_repr = data_repr
        self.VC = VC
        self.TF = TF
        self.jakkar = jakkar

        self.process_pool = None
        if max_processes:
            self.process_pool = multiprocessing.Pool(max_processes)

    def _get_data(
        self,
        semantic_path: str,
        raw_path: str,
        validation_path: str,
    ):
        data = self.data_repr.proccess(
            semantic_path,
            raw_path,
            validation_path,
        )

        data[DATA.VALIDATION_STATUS] = 0
        data[DATA.VALIDATED] = 1
        data[VENDOR_CODE.VALIDATED] = 1
        data[FEATURES.VALIDATED] = 1
        data[JAKKAR.VALIDATED] = 1
        data[FEATURES.NOT_FOUND] = ""
        return data

    def _process_validation(
        self,
        function: Callable,
        data: pd.DataFrame,
    ):
        new_data: pd.DataFrame
        new_data, columns = function(data, self.process_pool)

        data.loc[new_data.index, columns] = new_data[columns]
        return data

    def validate(
        self,
        semantic_path: str,
        raw_path: str,
        validation_path: str,
    ) -> pd.DataFrame:
        data = self._get_data(
            semantic_path,
            raw_path,
            validation_path,
        )

        if self.VC is not None:
            data = self._process_validation(self.VC.validate, data)

        if self.TF is not None:
            data = self._process_validation(self.TF.validate, data)

        if self.jakkar is not None:
            data = self._process_validation(self.jakkar.validate, data)

        if self.process_pool:
            self.process_pool.close()

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

    custom_features_list = [
        Weight,
        Volume,
        Quantity,
        Concentration,
        ConcentrationPerDose,
        FarmaForm,
    ]

    # VCExtractor = VendorCodeExtractor()
    data_repr = DataRepr(DataReprMode.VALIDATION)
    VC = VendorCodeSearch(True)
    TF = TextFeatureSearch(
        skip_validated=True,
        skip_intermediate_validated=False,
        custom_features_list=custom_features_list,
    )

    jakkar = FuzzyJakkarValidator(
        tokenizer=RegexTokenizer(
            {
                LanguageType.RUS: 1,
                LanguageType.ENG: 1,
            },
            weights_rules=RegexCustomWeights(1, 1, 1, 1),
        ),
        preprocessor=Preprocessor(2),
        fuzzy=FuzzySearch(75, transformer=TokenTransformer()),
        rate_counter=RateCounter(0, 1, 2, 0, RateFunction.sqrt2),
        marks_counter=MarksCounter(MarksMode.MULTIPLE),
        validation_treshold=0.5,
        debug=True,
    )

    validator = Validator(
        data_repr=data_repr,
        VC=None,
        TF=None,
        jakkar=jakkar,
    )

    start = time.time()
    result = validator.validate(
        semantic_path=None,
        raw_path=None,
        validation_path=r"C:\Users\tomilov-iv\Desktop\BrandPol\e2e4_validation.xlsx",
    )
    print("FINISHED IN", round(time.time() - start), "SECONDS")

    # metrics = Metric("Text Features", DATA.VALIDATED)
    # metrics.estimate(result)

    # jakkar_metrics = JakkarMetric(0.5)
    # jakkar_metrics.estimate(result)

    result.to_excel("e2e4_res.xlsx", index=False)
