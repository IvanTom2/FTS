from abc import ABC
from pathlib import Path
import os


class NOTATION(ABC):
    """Notation"""


# columns from validation BrandPol
class RAW(NOTATION):
    """Raw data file notation"""

    QUERY = "Запрос"
    ROW = "Строка валидации"
    LINK = "Ссылка"
    REGION = "Регион"
    MATCHED = "Сопоставление"
    MATCH_METHOD = "Метод"
    PRICE = "Цена"
    BRAND = "Бренд"
    NAME = "Наименование"
    VC = "Артикул"

    FAST_CHECK = "plus_word"
    MARK = "fast_check_mark"

    SOURCE = "Источник"


class SEMANTIC(NOTATION):
    """Semantic file notations"""

    NAME = "Название"
    QUERY = "Поисковый запрос"
    PLUS = "Плюс-слова"
    MINUS = "Минус-слова"
    REGEX = "Regex"
    NOTE = "Note"
    BARCODE = "Штрихкод"
    BRAND = "Brand"
    CATEGORY1 = "Категория"
    CATEGORY2 = "Категория 2"
    CLIENT_NAME = "Название клиента"
    CLIENT_IMG = "Ссылка на фото"
    VC = "Артикул"


class DATA(NOTATION):
    NAME = SEMANTIC.NAME
    LINK = RAW.LINK
    ROW = RAW.ROW
    QUERY = SEMANTIC.QUERY
    VC = SEMANTIC.VC
    CLIENT_NAME = "Наименование товара клиента"
    SOURCE_NAME = "Наименование товара на сайте"

    VALIDATED = "validated"

    @classmethod
    @property
    def rename(self):
        return {
            RAW.NAME: self.SOURCE_NAME,
            SEMANTIC.CLIENT_NAME: self.CLIENT_NAME,
        }

    @classmethod
    @property
    def raw_cols(self):
        return [RAW.NAME, RAW.LINK, RAW.ROW, RAW.QUERY]

    @classmethod
    @property
    def sem_cols(self):
        return [SEMANTIC.NAME, SEMANTIC.QUERY, SEMANTIC.CLIENT_NAME, SEMANTIC.VC]

    @classmethod
    @property
    def to_drop(self):
        return [RAW.QUERY]

    @classmethod
    @property
    def columns_order(self):
        return [
            self.NAME,
            self.QUERY,
            self.LINK,
            self.ROW,
            self.CLIENT_NAME,
            self.SOURCE_NAME,
            self.VC,
        ]


class VENDOR_CODE(NOTATION):
    """Vendor code notations"""

    COLUMN = "vendor_code"
    STATUS = "Валидация по артикулу"
    VALIDATED = "VC validation"

    @classmethod
    @property
    def TYPE(self):
        """
        Implemented types:
        1. ORIGINAL
        2. EXTRACTED
        """

        class TYPE(object):
            ORIGINAL = "Original VC"
            EXTRACTED = "Extracted VC"

        return TYPE

    @classmethod
    @property
    def TYPE_ERROR(self):
        class VendorCodeTypeError(NotImplementedError):
            pass

        return VendorCodeTypeError("This type of vendor code isn't implemented")


class FEATURES(NOTATION):
    """Text features notations"""

    NUMERICAL = "numerical_features"
    STRING = "string_features"

    CLIENT = "client_features"
    SOURCE = "source_features"
    CI = "CLIENT_INTERMEDIATE_FEATURES"
    SI = "SOURCE_INTERMEDIATE_FEATURES"

    INTERMEDIATE_VALIDATION = "intermediate_validation"

    STATUS = "Валидация по текстовым признакам"
    VALIDATED = "TF validation"

    @classmethod
    @property
    def DECISIVE(self):
        class DESICIVE(object):
            CLIENT = "client_desicive_features"
            SOURCE = "source_desicive_features"

        return DESICIVE


class JAKKAR(NOTATION):
    CLIENT = "_client"
    SOURCE = "_source"
    CLIENT_TOKENS = "_client_tokens"
    SOURCE_TOKENS = "_source_tokens"
    RATIO_PATH = r"ratio.xlsx"


class PATH(object):
    _main_path = ""
    _main_dir = "TEST_data"
    _farmacy = "Farmacy"
    _grocery = "Grocery"
    _technique = "Technique"

    @classmethod
    def file_path(self, file: str) -> str:
        files = os.listdir(self._main_path)
        _file = [f for f in files if file in f]

        if file == "raw":
            _file = [f for f in _file if "original" not in f][0]
        elif file == "semantic":
            _file = [f for f in _file if "_" not in f][0]
        else:
            _file = [f for f in files if file in f][0]

        path = self._main_path / _file
        return path

    @classmethod
    @property
    def Farmacy(self):
        class Farmacy(PATH):
            _main_path = Path(__file__).parent.parent / self._main_dir / self._farmacy

        return Farmacy

    @classmethod
    @property
    def Grocery(self):
        class Grocery(PATH):
            _main_path = Path(__file__).parent.parent / self._main_dir / self._grocery

        return Grocery

    @classmethod
    @property
    def Technique(self):
        class Technique(PATH):
            _main_path = Path(__file__).parent.parent / self._main_dir / self._technique

        return Technique


if __name__ == "__main__":
    print(PATH.Farmacy.file_path("semantic"))
