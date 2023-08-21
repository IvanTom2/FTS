import pandas as pd
import numpy as np
from pathlib import Path
import sys
from sklearn.metrics import accuracy_score, precision_score, recall_score

sys.path.append(str(Path(__file__).parent.parent))

from jakkar.jakkar import *
from main import *


class Metric(object):
    def __init__(
        self,
        type: str,
        pred_column: str,
    ) -> None:
        self._type = type
        self._pred_column = pred_column

    def estimate(self, data: pd.DataFrame) -> None:
        print(rf"\nEstimate Validator on {self._type}")
        print("-----------------------------------")

        prediction = data[self._pred_column]

        accuracy = accuracy_score(data["MyMark"], prediction)
        precision = precision_score(data["MyMark"], prediction)
        recall = recall_score(data["MyMark"], prediction)

        print(f"Accuracy = {round(accuracy, 2)}")
        print(f"Precision = {round(precision, 2)}")
        print(f"Recall = {round(recall, 2)}")

        print("-----------------------------------")


class JakkarMetric(object):
    def __init__(self, treshold: float) -> None:
        self.treshold = treshold

    def estimate(self, data: pd.DataFrame) -> None:
        labels = [MarksMode.CLIENT, MarksMode.SOURCE, MarksMode.UNION]

        for label in labels:
            if label in data.columns:
                print(f"\nEstimate FuzzyJakkar on {label}")
                print("-----------------------------------")

                prediction = np.where(
                    data[label] >= self.treshold,
                    1,
                    0,
                )

                accuracy = accuracy_score(data["MyMark"], prediction)
                precision = precision_score(data["MyMark"], prediction)
                recall = recall_score(data["MyMark"], prediction)

                print(f"Accuracy = {round(accuracy, 2)}")
                print(f"Precision = {round(precision, 2)}")
                print(f"Recall = {round(recall, 2)}")

                print("-----------------------------------")


def jakkar_setup():
    # path = "/home/mainus/Data Sets/Аптеки/TESTDATA_farmaimpex.xlsx"
    path = "/home/mainus/Data Sets/Продукты/TESTDATA_vkusvill.xlsx"

    data = pd.read_excel(path)

    tokenizer = BasicTokenizer()
    preprocessor = Preprocessor(2)
    transformer = TokenTransformer()
    rate_counter = RateCounter(rate_function=RateFunction.default)
    fuzzy = FuzzySearch(65, transformer=transformer)
    marks_counter = MarksCounter(MarksMode.MULTIPLE)

    validator = FuzzyJakkarValidator(
        tokenizer=tokenizer,
        preprocessor=preprocessor,
        fuzzy=fuzzy,
        rate_counter=rate_counter,
        marks_counter=marks_counter,
        debug=True,
    )

    data, cols = validator.validate(data)

    JM = JakkarMetric(0.5)
    JM.estimate(data)


def vendor_code_setup():
    spath = "/home/mainus/BrandPol/semantic.xlsx"
    vpath = "/home/mainus/BrandPol/validation.xlsx"

    validator = Validator(
        DataRepr(DataReprMode.DEFAULT),
        VendorCodeExtractor(),
        VendorCodeSearch(),
        None,
        None,
    )

    data = validator.validate(spath, vpath)

    metric = Metric("Vendor Code", VENDOR_CODE.VALIDATED)
    metric.estimate(data)


def features_setup():
    spath = "/home/mainus/BrandPol/semantic.xlsx"
    vpath = "/home/mainus/BrandPol/validation.xlsx"

    validator = Validator(
        DataRepr(DataReprMode.DEFAULT),
        VendorCodeExtractor(),
        None,
        TextFeatureSearch(),
        None,
    )

    data = validator.validate(spath, vpath)

    metric = Metric("Features", FEATURES.VALIDATED)
    metric.estimate(data)


if __name__ == "__main__":
    jakkar_setup()
