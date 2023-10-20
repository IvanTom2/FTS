import pandas as pd
import numpy as np
from notation import DATA, PRICE_VALIDATOR
import warnings

warnings.filterwarnings("ignore")


class PriceValidator(object):
    def __init__(self):
        pass

    def _price_func(
        self,
        df: pd.DataFrame,
        mark: int,
    ) -> pd.Series:
        if len(df) > 1:
            df[PRICE_VALIDATOR.PRICE_DIFF] = abs(
                df[DATA.CLIENT_PRICE] - df[DATA.SOURCE_PRICE]
            )
            df[PRICE_VALIDATOR.PRICE_VALID] = np.where(
                df[PRICE_VALIDATOR.PRICE_DIFF] == df[PRICE_VALIDATOR.PRICE_DIFF].min(),
                mark,
                0,
            )
        return df

    def _groupby_goods(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.groupby(
            by=[
                DATA.NAME,
                DATA.SOURCE,
                DATA.REGION,
            ],
        ).apply(self._price_func, PRICE_VALIDATOR.GOODS_MARK)

        return data

    def _groupby_links(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.groupby(
            by=[
                DATA.LINK,
                DATA.REGION,
            ],
        ).apply(self._price_func, PRICE_VALIDATOR.LINKS_MARK)

        return data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        data[PRICE_VALIDATOR.PRICE_VALID] = 90  # by default assumption
        data[DATA.REGION] = data[DATA.REGION].fillna("")

        data = self._groupby_goods(data)
        data = self._groupby_links(data)

        data = data.drop(PRICE_VALIDATOR.PRICE_DIFF, axis=1)
        return data


if __name__ == "__main__":
    priceV = PriceValidator()
    data = pd.read_excel(r"C:\Users\tomilov-iv\Desktop\BrandPol\letual.xlsx")

    data = priceV.validate(data)
    data.to_excel("output.xlsx", index=False)
