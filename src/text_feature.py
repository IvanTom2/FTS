import pandas as pd
from abc import ABC, abstractmethod
import warnings
import numpy as np
from typing import Union
import regex as re
from tqdm import tqdm
import multiprocessing

tqdm.pandas()

from notation import DATA, FEATURES
from features_collection import (
    AbstractFeature,
    FutureList,
    Measure,
    Type,
    FeatureValidationMode,
    FeatureNotFoundMode,
    NotFoundStatus,
)

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
        custom_features_list: list[AbstractFeature] = [],
    ) -> None:
        self.skip_validated = skip_validated
        self.skip_intermediate_validated = skip_intermediate_validated

        self.futures = FutureList(custom_features_list)

    def _preproccess(
        self,
        values: list[str],
        feature: AbstractFeature,
        measure: Measure,
    ) -> list:
        return [feature(value, measure) for value in values]

    def _findall(self, row: str, rx: str) -> list[str]:
        row = str(row)
        output = re.findall(rx, row, re.IGNORECASE)
        return output

    def _feature_search(
        self,
        series: pd.Series,
        feature: AbstractFeature,
        designation: Union[Measure, Type],
        progress_for: str,
    ) -> pd.Series:
        # print("Search features", progress_for, feature.NAME)
        series = series.apply(self._findall, args=(designation.regex,))
        series = series.apply(self._preproccess, args=(feature, designation))
        return series

    def _determine_based_intersection(
        self,
        cif: set,
        sif: set,
        val_mode: FeatureValidationMode,
    ) -> int:
        if val_mode is FeatureValidationMode.MODEST:
            based = min(len(cif), len(sif))
        elif val_mode is FeatureValidationMode.CLIENT:
            based = len(cif)
        elif val_mode is FeatureValidationMode.SOURCE:
            based = len(sif)
        else:  # val_mode is FeatureValidationMode.STRICT
            based = max(len(cif), len(sif))
        return based

    def _old_intermediate_validation(
        self,
        row: pd.Series,
        val_mode: FeatureValidationMode,
        not_found_mode: FeatureNotFoundMode,
        feature_name: str,
    ) -> pd.Series:
        if row[FEATURES.INTERMEDIATE_VALIDATION] == 1:
            cif = set(row[FEATURES.CI])  # client intermediate features
            sif = set(row[FEATURES.SI])  # source intermediate features

            not_found_status = NotFoundStatus(cif, sif, not_found_mode, feature_name)
            if not_found_status:
                row[FEATURES.INTERMEDIATE_VALIDATION] = not_found_status.desicion
                row[FEATURES.NOT_FOUND] += not_found_status.status
                return row

            based = self._determine_based_intersection(cif, sif, val_mode)
            intersect = cif.intersection(sif)
            desicion = 1 if len(intersect) == based else 0

            if desicion == 1:
                row[FEATURES.INTERMEDIATE_VALIDATION] = row[
                    FEATURES.INTERMEDIATE_VALIDATION
                ]
            else:
                row[FEATURES.INTERMEDIATE_VALIDATION] = 0

            row[FEATURES.NOT_FOUND] = f"Feature {feature_name} founded; "

        return row

    def _intermediate_validation_func(self, row: list[Union[int, set]]) -> list[int]:
        desicion = row[0]
        if desicion == 1:
            cif = row[1]
            sif = row[2]

            not_found_status = NotFoundStatus(
                cif,
                sif,
                self.__not_found_mode,
                self.__feature_name,
            )

            if not_found_status:
                desicion = not_found_status.desicion
                return desicion

            based = self._determine_based_intersection(cif, sif, self.__val_mode)
            intersect = cif.intersection(sif)
            desicion = desicion if len(intersect) == based else 0

        return desicion

    def _intermediate_validation(
        self,
        data: pd.DataFrame,
        feature: AbstractFeature,
    ) -> pd.DataFrame:
        cif_massive = map(set, data[FEATURES.CI].to_list())
        sif_massive = map(set, data[FEATURES.SI].to_list())
        intermediate = data[FEATURES.INTERMEDIATE_VALIDATION].to_list()

        self.__not_found_mode = feature.NOT_FOUND_MODE
        self.__feature_name = feature.NAME
        self.__val_mode = feature.VALIDATION_MODE

        massive = list(zip(intermediate, cif_massive, sif_massive))
        desicions = list(map(self._intermediate_validation_func, tqdm(massive)))
        data[FEATURES.INTERMEDIATE_VALIDATION] = desicions

        return data

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
                FEATURES.NOT_FOUND,
            ],
        ] = new_data[
            [
                FEATURES.STATUS,
                FEATURES.INTERMEDIATE_VALIDATION,
                FEATURES.NOT_FOUND,
            ]
        ]
        return data

    def _extract(self, data: pd.DataFrame) -> pd.DataFrame:
        cur_df = data[:]  # current working dataframe

        for feature in self.futures:
            feature: AbstractFeature

            cur_df[FEATURES.CI] = [[] for _ in range(len(cur_df))]
            cur_df[FEATURES.SI] = [[] for _ in range(len(cur_df))]

            for designation in feature.designations:
                cif = self._feature_search(
                    cur_df[DATA.CLIENT_NAME],
                    feature,
                    designation,
                    progress_for="client",
                )
                sif = self._feature_search(
                    cur_df[DATA.ROW],
                    feature,
                    designation,
                    progress_for="source",
                )

                cur_df[FEATURES.CI] += cif
                cur_df[FEATURES.SI] += sif

            cur_df = self._intermediate_validation(cur_df, feature)

            # cur_df = cur_df.progress_apply(
            #     self._old_intermediate_validation,
            #     axis=1,
            #     args=(
            #         feature.VALIDATION_MODE,
            #         feature.NOT_FOUND_MODE,
            #         feature.NAME,
            #     ),
            # )

            cur_df.loc[
                cur_df[FEATURES.INTERMEDIATE_VALIDATION] == 0,
                FEATURES.STATUS,
            ] = f"Not validated by {feature.NAME}"

            if self.skip_intermediate_validated:
                cur_df = cur_df[cur_df[FEATURES.INTERMEDIATE_VALIDATION] == 1]

            data = self._hand_over_features(data, cur_df)
        data = self._hand_over_intermediate(data, cur_df)

        return data

    def validate(
        self,
        data: pd.DataFrame,
        process_pool: multiprocessing.Pool,
    ) -> pd.DataFrame:
        if self.skip_validated:
            data = data[data[DATA.VALIDATION_STATUS] == 0]

        data[FEATURES.STATUS] = ""
        data[FEATURES.INTERMEDIATE_VALIDATION] = 1

        data.loc[:, FEATURES.CLIENT] = [[] for _ in range(len(data))]
        data.loc[:, FEATURES.SOURCE] = [[] for _ in range(len(data))]

        data = self._extract(data)

        data[DATA.VALIDATED] = np.where(
            data[FEATURES.INTERMEDIATE_VALIDATION] == 1,
            data[DATA.VALIDATED],
            0,
        )

        # data.loc[data[FEATURES.INTERMEDIATE_VALIDATION] == 1, DATA.VALIDATED] = 1
        data.loc[
            data[FEATURES.INTERMEDIATE_VALIDATION] == 1,
            FEATURES.STATUS,
        ] = "Validated"

        data[FEATURES.VALIDATED] = np.where(
            data[FEATURES.STATUS] == "Validated",
            data[FEATURES.VALIDATED],
            0,
        )

        return data, [
            DATA.VALIDATED,
            FEATURES.STATUS,
            FEATURES.VALIDATED,
            FEATURES.CLIENT,
            FEATURES.SOURCE,
            FEATURES.NOT_FOUND,
        ]


if __name__ == "__main__":
    pass
