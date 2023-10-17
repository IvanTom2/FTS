import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Union
import re
import multiprocessing

from notation import SEMANTIC, VENDOR_CODE, DATA


class VendorCode(object):
    SYMBOLS_TO_CHANGE_IN_VC = r"-|\\|\.|\/"
    SYMBOLS_CHANGE_TO = r".?"

    def __init__(
        self,
        value: str,
        type_: VENDOR_CODE.TYPE,
    ) -> None:
        if type_ not in [
            VENDOR_CODE.TYPE.ORIGINAL,
            VENDOR_CODE.TYPE.EXTRACTED,
        ]:
            raise VENDOR_CODE.TYPE_ERROR

        self.value = value
        self.rx = self._get_rx()
        self.type = type_

    def _get_rx(self) -> str:
        if self.value:
            value = re.sub(
                self.SYMBOLS_TO_CHANGE_IN_VC,
                self.SYMBOLS_CHANGE_TO,
                self.value,
            )
        return value

    def __repr__(self) -> str:
        return str(self.value)

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    @property
    def regex(self):
        RX = r"(?:[a-z0-9]+[-.\\\/])+[a-z0-9]*"
        return RX


class VendorCodeExtractor(object):
    def __init__(self) -> None:
        pass

    def _transform_to_VC(
        self,
        series: pd.Series,
        type: VENDOR_CODE.TYPE,
    ) -> pd.Series:
        def transform(cell: Union[str, list]) -> list:
            if isinstance(cell, str):
                return [VendorCode(cell, type)]
            elif isinstance(cell, list):
                return [VendorCode(element, type) for element in cell]

        return series.apply(transform)

    def _extract_vendor_code(
        self,
        series: pd.Series,
    ) -> pd.Series:
        series = series.str.findall(VendorCode.regex)
        return self._transform_to_VC(series, VENDOR_CODE.TYPE.EXTRACTED)

    def extract(self, semantic: pd.DataFrame) -> pd.DataFrame:
        semantic[SEMANTIC.VC] = np.where(
            semantic[SEMANTIC.VC].isna(),
            self._extract_vendor_code(semantic[SEMANTIC.CLIENT_NAME]),
            self._transform_to_VC(
                semantic[SEMANTIC.VC],
                VENDOR_CODE.TYPE.ORIGINAL,
            ),
        )
        return semantic


class AbstractVendorCodeSearch(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(
        self,
        data: pd.DataFrame,
    ) -> tuple[pd.DataFrame, list[str]]:
        pass


class VendorCodeSearch(AbstractVendorCodeSearch):
    def __init__(
        self,
        skip_validated: bool = True,
    ) -> None:
        self.skip_validated = skip_validated

    def _validate(self, row: pd.Series) -> pd.Series:
        if row[DATA.VC]:
            for vendor_code in row[DATA.VC]:
                vendor_code: VendorCode
                if re.search(
                    vendor_code.rx,
                    row[DATA.ROW],
                    flags=re.IGNORECASE,
                ):
                    row[DATA.VALIDATION_STATUS] = 1
                    row[DATA.VALIDATED] = row[DATA.VALIDATED]
                    row[VENDOR_CODE.VALIDATED] = row[VENDOR_CODE.VALIDATED]
                    row[VENDOR_CODE.STATUS] = f"Validated by {vendor_code.type}"
                    return row
                continue

            row[DATA.VALIDATION_STATUS] = 1
            row[DATA.VALIDATED] = 0
            row[VENDOR_CODE.VALIDATED] = 0
            row[VENDOR_CODE.STATUS] = "Not validated"
            return row

        row[DATA.VALIDATED] = row[DATA.VALIDATED]
        row[VENDOR_CODE.VALIDATED] = row[VENDOR_CODE.VALIDATED]
        row[VENDOR_CODE.STATUS] = "No vendor code"
        return row

    def validate(
        self,
        data: pd.DataFrame,
        process_pool: multiprocessing.Pool,
    ) -> pd.DataFrame:
        if self.skip_validated:
            data = data[data[DATA.VALIDATION_STATUS] == 0]

        data = data.apply(self._validate, axis=1)
        return data, [DATA.VALIDATED, VENDOR_CODE.VALIDATED, VENDOR_CODE.STATUS]
