import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Union
import re

from notation import *

ORIGINAL_VC = "Original VC"
EXTRACTED_VC = "Extracted VC"


class VendorCodeTypeError(Exception):
    pass


class VendorCode(object):
    SYMBOLS_TO_CHANGE_IN_VC = r"-|\\|\.|\/"
    SYMBOLS_CHANGE_TO = r".?"

    def __init__(
        self,
        value: str,
        type_: str,
    ) -> None:
        self.value = value
        self.rx = self._get_rx()

        if type_ not in [ORIGINAL_VC, EXTRACTED_VC]:
            raise VendorCodeTypeError
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


class VendorCodeExtractor(object):
    VCRX = r"(?:[a-z0-9]+[-.\\\/])+[a-z0-9]*"

    def __init__(self) -> None:
        pass

    def _transform_to_VC(self, series: pd.Series, type: str) -> pd.Series:
        def transform(cell: Union[str, list]) -> list:
            if isinstance(cell, str):
                return [VendorCode(cell, type)]
            elif isinstance(cell, list):
                return [VendorCode(element, type) for element in cell]

        return series.apply(transform)

    def _extract_vendor_code(self, series: pd.Series) -> pd.Series:
        series = series.str.findall(self.VCRX)
        return self._transform_to_VC(series, EXTRACTED_VC)

    def extract(self, semantic: pd.DataFrame) -> pd.DataFrame:
        semantic[VENDOR_CODE] = np.where(
            semantic[VENDOR_CODE].isna(),
            self._extract_vendor_code(semantic[SEMANTIC_CLIENT_NAME]),
            self._transform_to_VC(semantic[VENDOR_CODE], ORIGINAL_VC),
        )
        return semantic


class AbstractVendorCodeSearch(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(self, data: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        pass


class VendorCodeSearch(AbstractVendorCodeSearch):
    def __init__(
        self,
        skip_validated: bool = True,
    ) -> None:
        self.skip_validated = skip_validated

    def _validate(self, row: pd.Series) -> pd.Series:
        if row[VENDOR_CODE]:
            for vendor_code in row[VENDOR_CODE]:
                vendor_code: VendorCode
                if re.search(
                    vendor_code.rx,
                    row[VALIDATION_ROW],
                    flags=re.IGNORECASE,
                ):
                    row[VALIDATED] = 1
                    row[VC_STATUS] = f"Validated by {vendor_code.type}"
                    return row
                continue

            row[VALIDATED] = 0
            row[VC_STATUS] = "Not validated"
            return row

        row[VALIDATED] = 0
        row[VC_STATUS] = "No vendor code"
        return row

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.skip_validated:
            data = data[data[VALIDATED] == 0]

        data = data.apply(self._validate, axis=1)
        return data, [VALIDATED, VC_STATUS]
