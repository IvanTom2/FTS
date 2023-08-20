from pathlib import Path
import sys
import pytest
import re
import pandas as pd


sys.path.append(str(Path(__file__).parent.parent))

from vendor_code import (
    VendorCode,
    ORIGINAL_VC,
    EXTRACTED_VC,
    VendorCodeTypeError,
    VendorCodeExtractor,
    VendorCodeSearch,
)
from notation import VALIDATION_ROW, VALIDATED, VENDOR_CODE


def test_vendor_code_type():
    txt = "km-1-3.29m"

    vc1 = VendorCode(txt, ORIGINAL_VC)
    vc2 = VendorCode(txt, EXTRACTED_VC)

    with pytest.raises(VendorCodeTypeError):
        vc3 = VendorCode(txt, "SomeType")


def test_vendor_code_extractor_rx():
    rx = VendorCodeExtractor.VCRX

    goods = [
        "товар 1 зеленый код ax-13.59.0",
        "книга Печать Зла 978-5-17-118103-1",
    ]

    needed_vendor_codes_values = [
        ["ax-13.59.0"],
        ["978-5-17-118103-1"],
    ]

    vendor_codes = [re.findall(rx, good) for good in goods]
    assert vendor_codes == needed_vendor_codes_values


def test_vendor_code_rx():
    type_ = ORIGINAL_VC

    vcodes = [
        "ax-13.59.0",
        "978-5-17-118103-1",
    ]

    needed_vendor_codes_rxs = [
        "ax.?13.?59.?0",
        "978.?5.?17.?118103.?1",
    ]

    vendor_codes_rxs = [VendorCode(vcode, type_).rx for vcode in vcodes]
    assert vendor_codes_rxs == needed_vendor_codes_rxs


def test_vendor_code_search():
    type_ = ORIGINAL_VC
    data = pd.DataFrame(
        data=[
            [
                [VendorCode("ax.?13.?59.?0", type_)],
                "good1 ax-13.59.0",
                0,
                1,
            ],
            [
                [VendorCode("978.?5.?17.?118103.?1", type_)],
                "good2 978-5-17-118103-1",
                0,
                1,
            ],
        ],
        columns=[VENDOR_CODE, VALIDATION_ROW, VALIDATED, "MyMark"],
    )

    VC = VendorCodeSearch()
    val = VC.validate(data)[0]

    result = val[VALIDATED] - val["MyMark"]
    assert result.sum() == 0


if __name__ == "__main__":
    test_vendor_code_search()
