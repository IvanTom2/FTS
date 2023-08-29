from pathlib import Path
import sys
import pytest
import re
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from vendor_code import (
    VendorCode,
    VendorCodeExtractor,
    VendorCodeSearch,
)
from notation import RAW, DATA, VENDOR_CODE


def test_vendor_code_type():
    txt = "km-1-3.29m"

    vc1 = VendorCode(txt, VENDOR_CODE.TYPE.ORIGINAL)
    vc2 = VendorCode(txt, VENDOR_CODE.TYPE.EXTRACTED)

    with pytest.raises(Exception):
        vc3 = VendorCode(txt, "SomeType")


def test_vendor_code_extractor_rx():
    rx = VendorCode.regex

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
    type_ = VENDOR_CODE.TYPE.ORIGINAL

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
    type_ = VENDOR_CODE.TYPE.ORIGINAL

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
        columns=[DATA.VC, DATA.ROW, DATA.VALIDATED, "MyMark"],
    )

    VC = VendorCodeSearch()
    val = VC.validate(data)[0]

    result = val[DATA.VALIDATED] - val["MyMark"]
    assert result.sum() == 0


if __name__ == "__main__":
    test_vendor_code_search()
