import pandas as pd
from abc import ABC, abstractmethod
import warnings

from notation import *
from features_collection import *

warnings.filterwarnings("ignore")


class AbstractTextFeatureSearch(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


class TextFeatureSearch(AbstractTextFeatureSearch):
    ALL_FEATURES = [Weight, Volume, MemoryCapacity]  #

    def __init__(
        self,
        skip_validated: bool = True,
        skip_intermediate_validated: bool = True,
        decisive_features: str = CLIENT_DESICIVE_FEATURES,
    ) -> None:
        self.skip_validated = skip_validated
        self.skip_intermediate_validated = skip_intermediate_validated

        assert decisive_features in [
            CLIENT_DESICIVE_FEATURES,
            SOURCE_DESICIVE_FEATURES,
        ], f"Desicive features should be {CLIENT_DESICIVE_FEATURES}, {SOURCE_DESICIVE_FEATURES}"
        self.decisive_features = decisive_features

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
        cif = set(row[CIF])  # client intermediate features
        sif = set(row[SIF])  # source intermediate features
        intersect = cif.intersection(sif)

        min_intersect_len = min(len(cif), len(sif))
        desicion = 1 if len(intersect) == min_intersect_len else 0

        row[INTERMEDIATE_VALIDATION] = desicion
        return row

    def _hand_over_features(
        self,
        data: pd.DataFrame,
        new_data: pd.DataFrame,
    ) -> pd.DataFrame:
        data.loc[new_data.index, CLIENT_FEATURES] += new_data[CIF]
        data.loc[new_data.index, SOURCE_FEATURES] += new_data[SIF]
        return data

    def _hand_over_intermediate(
        self,
        data: pd.DataFrame,
        new_data: pd.DataFrame,
    ) -> pd.DataFrame:
        data.loc[new_data.index, [TF_STATUS, INTERMEDIATE_VALIDATION]] = new_data[
            [
                TF_STATUS,
                INTERMEDIATE_VALIDATION,
            ]
        ]
        return data

    def _extract(self, data: pd.DataFrame) -> pd.DataFrame:
        cur_df = data[:]  # current working dataframe

        for feature in self.ALL_FEATURES:
            cur_df[CIF] = [[] for _ in range(len(cur_df))]
            cur_df[SIF] = [[] for _ in range(len(cur_df))]

            for RX in feature.RXS:
                cli = self._feature_search(cur_df[VALIDATION_CLIENT_NAME], feature, RX)
                src = self._feature_search(cur_df[VALIDATION_ROW], feature, RX)

                cur_df[CIF] += cli
                cur_df[SIF] += src

            cur_df = cur_df.apply(self._intermediate_validation, axis=1)
            cur_df.loc[
                cur_df[INTERMEDIATE_VALIDATION] == 0, TF_STATUS
            ] = f"Not validated by {feature.NAME}"

            data = self._hand_over_features(data, cur_df)
            if self.skip_intermediate_validated:
                data = self._hand_over_intermediate(data, cur_df)
                cur_df = cur_df[cur_df[INTERMEDIATE_VALIDATION] != 0]

        return data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.skip_validated:
            data = data[data[VALIDATED] == 0]

        data[TF_STATUS] = ""
        data[INTERMEDIATE_VALIDATION] = 1

        data.loc[:, CLIENT_FEATURES] = [[] for _ in range(len(data))]
        data.loc[:, SOURCE_FEATURES] = [[] for _ in range(len(data))]

        data = self._extract(data)

        data.loc[data[INTERMEDIATE_VALIDATION] == 1, VALIDATED] = 1
        data.loc[data[INTERMEDIATE_VALIDATION] == 1, TF_STATUS] = "Validated"

        return data, [VALIDATED, TF_STATUS, CLIENT_FEATURES, SOURCE_FEATURES]
