import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm
from notation import NOTATION, DATA, GROUPBY_VALIDATOR
from typing import Callable
from functools import partial


tqdm.pandas()
warnings.filterwarnings("ignore")


class AGR_FUNC(object):
    MAX = "max"
    MIN = "min"


class GroupByValidator(object):
    """
    Can validate grouped position by price or other features.
    Pass features as: feature_name = [column_name, agr_func].
    Agr_funcs: AGR_FUNC = {"min", "max"}.
    """

    def __init__(
        self,
        price_validation: bool = True,
        **features: dict[list[NOTATION, AGR_FUNC]],
    ):
        self.price_validation = price_validation
        self.features = features

    def _price_func(
        self,
        df: pd.DataFrame,
        mark: int,
    ) -> pd.Series:
        if len(df) > 1:
            df[GROUPBY_VALIDATOR.PRICE_DIFF] = abs(
                df[DATA.CLIENT_PRICE] - df[DATA.SOURCE_PRICE]
            )
            df[GROUPBY_VALIDATOR.PRICE_VALID] = np.where(
                df[GROUPBY_VALIDATOR.PRICE_DIFF]
                == df[GROUPBY_VALIDATOR.PRICE_DIFF].min(),
                mark,
                0,
            )
        return df

    def _feature_func(
        self,
        df: pd.DataFrame,
        mark: int,
        output_column: str,
        work_column: str,
        agr_func: str,
    ) -> pd.Series:
        if len(df) > 1:
            if agr_func == AGR_FUNC.MAX:
                df[output_column] = np.where(
                    df[work_column] == df[work_column].min(),
                    mark,
                    0,
                )
            else:
                df[output_column] = np.where(
                    df[work_column] == df[work_column].max(),
                    mark,
                    0,
                )
        return df

    def _groupby_goods(self, data: pd.DataFrame, func: Callable) -> pd.DataFrame:
        data = data.groupby(
            by=[
                DATA.NAME,
                DATA.SOURCE,
                DATA.REGION,
            ],
        ).progress_apply(func, mark=GROUPBY_VALIDATOR.GOODS_MARK)

        return data

    def _groupby_links(self, data: pd.DataFrame, func: Callable) -> pd.DataFrame:
        data = data.groupby(
            by=[
                DATA.LINK,
                DATA.REGION,
            ],
        ).progress_apply(func, mark=GROUPBY_VALIDATOR.LINKS_MARK)

        return data

    def validate_by_price(self, data: pd.DataFrame) -> pd.DataFrame:
        data[GROUPBY_VALIDATOR.PRICE_VALID] = 90  # by default assumption
        data[DATA.REGION] = data[DATA.REGION].fillna("")

        data = self._groupby_goods(data, self._price_func)
        data = self._groupby_links(data, self._price_func)

        data = data.drop(GROUPBY_VALIDATOR.PRICE_DIFF, axis=1)
        return data

    def validate_by_feature(
        self,
        data: pd.DataFrame,
        feature_name: str,
        work_column: str,
        agr_func: AGR_FUNC,
    ) -> pd.DataFrame:
        output_column = feature_name + "_groupby_validation"

        data[output_column] = 90  # by default assumption
        data[DATA.REGION] = data[DATA.REGION].fillna("")

        func = partial(
            self._feature_func,
            output_column=output_column,
            work_column=work_column,
            agr_func=agr_func,
        )

        data = self._groupby_goods(data, func)
        data = self._groupby_links(data, func)

        return data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.price_validation:
            data = self.validate_by_price(data)

        if self.features:
            for feature in self.features.keys():
                work_column = self.features[feature][0]
                agr_func = self.features[feature][1]

                data = self.validate_by_feature(
                    data,
                    feature,
                    work_column,
                    agr_func,
                )

        return data


if __name__ == "__main__":
    priceV = GroupByValidator(
        price_validation=True,
        jakkar=["marks_union", AGR_FUNC.MIN],
    )

    data = pd.read_excel(r"C:\Users\tomilov-iv\Desktop\BrandPol\HandValidation.xlsx")

    data = priceV.validate(data)
    data.to_excel("output.xlsx", index=False)
