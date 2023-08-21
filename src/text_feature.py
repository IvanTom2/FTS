import pandas as pd
from abc import ABC, abstractmethod
import warnings
import numpy as np


from notation import *
from features_collection import AbstractFeature, Sign, Rx, ALL_FUTURES

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
    ) -> None:
        self.skip_validated = skip_validated
        self.skip_intermediate_validated = skip_intermediate_validated

    def _preproccess(
        self,
        values: list[str],
        feature: AbstractFeature,
        sign: Sign,
    ) -> list:
        return [feature(value, sign) for value in values]

    def _feature_search(
        self,
        series: pd.Series,
        feature: AbstractFeature,
        RX: Rx,
    ) -> pd.Series:
        series = series.str.findall(RX.rx)
        series = series.apply(self._preproccess, args=(feature, RX.sign))
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

        for feature in ALL_FUTURES:
            cur_df[FEATURES.CI] = [[] for _ in range(len(cur_df))]
            cur_df[FEATURES.SI] = [[] for _ in range(len(cur_df))]

            for regex in feature.RXS:
                cif = self._feature_search(
                    cur_df[VALIDATION.CLIENT_NAME],
                    feature,
                    regex,
                )
                sif = self._feature_search(
                    cur_df[VALIDATION.VALIDATION_ROW],
                    feature,
                    regex,
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
            data = data[data[VALIDATED] == 0]

        data[FEATURES.STATUS] = ""
        data[FEATURES.INTERMEDIATE_VALIDATION] = 1

        data.loc[:, FEATURES.CLIENT] = [[] for _ in range(len(data))]
        data.loc[:, FEATURES.SOURCE] = [[] for _ in range(len(data))]

        data = self._extract(data)

        data.loc[data[FEATURES.INTERMEDIATE_VALIDATION] == 1, VALIDATED] = 1
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
            VALIDATED,
            FEATURES.STATUS,
            FEATURES.VALIDATED,
            FEATURES.CLIENT,
            FEATURES.SOURCE,
        ]