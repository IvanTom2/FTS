import sys
from pathlib import Path
import pandas as pd
import numpy as np


sys.path.append(str(Path(__file__).parent.parent))
from features_collection import NumericalFeature, Weight
from text_feature import TextFeatureSearch, AbstractFeature, Rx


def test_numerical(
    data: pd.DataFrame,
    feature: AbstractFeature,
    regex: Rx,
) -> pd.DataFrame:
    output = TextFeatureSearch()._feature_search(
        data,
        feature,
        regex,
    )
    return output


def test_weight():
    data = pd.DataFrame(
        data=[
            [],
            [],
        ],
        columns=[],
    )

    output = test_numerical(data, Weight, Weight.RXS[0])


if __name__ == "__main__":
    pass
