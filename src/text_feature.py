import pandas as pd
from abc import ABC, abstractmethod
import warnings
import numpy as np


from notation import DATA, FEATURES
from features_collection import AbstractFeature, Sign, Rx, ALL_FUTURES, Measure

warnings.filterwarnings("ignore")


class AbstractTextFeatureSearch(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


class TextFeatureSearch(AbstractTextFeatureSearch):
    def __init__(
        self,
        skip_validated: bool = True,
        skip_intermediate_validated: bool = True,
        custom_features_list: list = [],
    ) -> None:
        self.skip_validated = skip_validated
        self.skip_intermediate_validated = skip_intermediate_validated

        self.ALL_FUTURES = custom_features_list if custom_features_list else ALL_FUTURES

    def _preproccess(
        self,
        values: list[str],
        feature: AbstractFeature,
        measure: Measure,
    ) -> list:
        return [feature(value, measure) for value in values]

    def _feature_search(
        self,
        series: pd.Series,
        feature: AbstractFeature,
        measure: Measure,
    ) -> pd.Series:
        series = series.str.findall(measure.regex)
        series = series.apply(self._preproccess, args=(feature, measure))
        return series

    def _intermediate_validation(self, row: pd.Series) -> pd.Series:
        cif = set(row[FEATURES.CI])  # client intermediate features
        sif = set(row[FEATURES.SI])  # source intermediate features
        intersect = cif.intersection(sif)

        min_intersect_len = min(len(cif), len(sif))
        desicion = 1 if len(intersect) == min_intersect_len else 0

        row[FEATURES.INTERMEDIATE_VALIDATION] = desicion
        return row

    def _hand_over_features(
        self,
        data: pd.DataFrame,
        new_data: pd.DataFrame,
    ) -> pd.DataFrame:
        data.loc[new_data.index, FEATURES.CLIENT] += new_data[FEATURES.CI]
        data.loc[new_data.index, FEATURES.SOURCE] += new_data[FEATURES.SI]
        return data

    def _hand_over_intermediate(
        self,
        data: pd.DataFrame,
        new_data: pd.DataFrame,
    ) -> pd.DataFrame:
        data.loc[
            new_data.index,
            [
                FEATURES.STATUS,
                FEATURES.INTERMEDIATE_VALIDATION,
            ],
        ] = new_data[
            [
                FEATURES.STATUS,
                FEATURES.INTERMEDIATE_VALIDATION,
            ]
        ]
        return data

    def _extract(self, data: pd.DataFrame) -> pd.DataFrame:
        cur_df = data[:]  # current working dataframe

        for feature in self.ALL_FUTURES:
            cur_df[FEATURES.CI] = [[] for _ in range(len(cur_df))]
            cur_df[FEATURES.SI] = [[] for _ in range(len(cur_df))]

            for measure in feature.MEASURES:
                cif = self._feature_search(
                    cur_df[DATA.CLIENT_NAME],
                    feature,
                    measure,
                )
                sif = self._feature_search(
                    cur_df[DATA.ROW],
                    feature,
                    measure,
                )

                cur_df[FEATURES.CI] += sif
                cur_df[FEATURES.SI] += cif

            cur_df = cur_df.apply(self._intermediate_validation, axis=1)
            cur_df.loc[
                cur_df[FEATURES.INTERMEDIATE_VALIDATION] == 0,
                FEATURES.STATUS,
            ] = f"Not validated by {feature.NAME}"

            data = self._hand_over_features(data, cur_df)
            if self.skip_intermediate_validated:
                data = self._hand_over_intermediate(data, cur_df)
                cur_df = cur_df[cur_df[FEATURES.INTERMEDIATE_VALIDATION] != 0]

        return data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.skip_validated:
            data = data[data[DATA.VALIDATED] == 0]

        data[FEATURES.STATUS] = ""
        data[FEATURES.INTERMEDIATE_VALIDATION] = 1

        data.loc[:, FEATURES.CLIENT] = [[] for _ in range(len(data))]
        data.loc[:, FEATURES.SOURCE] = [[] for _ in range(len(data))]

        data = self._extract(data)

        data.loc[data[FEATURES.INTERMEDIATE_VALIDATION] == 1, DATA.VALIDATED] = 1
        data.loc[
            data[FEATURES.INTERMEDIATE_VALIDATION] == 1,
            FEATURES.STATUS,
        ] = "Validated"

        data[FEATURES.VALIDATED] = np.where(
            data[FEATURES.STATUS] == "Validated",
            1,
            0,
        )

        return data, [
            DATA.VALIDATED,
            FEATURES.STATUS,
            FEATURES.VALIDATED,
            FEATURES.CLIENT,
            FEATURES.SOURCE,
        ]


if __name__ == "__main__":
    pass
