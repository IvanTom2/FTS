from abc import ABC


class NOTATION(ABC):
    """Notation"""


# columns from validation BrandPol
class VALIDATION(NOTATION):
    """Validation file notation"""

    NAME = "Наименование"
    SOURCE = "Сайт"
    LINK = "Ссылка"
    REGION = "Регион"
    LINK_FROM = "Источник ссылки"
    DATE_FOUNDED = r"Дата первого\nобнаружения\nссылки"
    DATE_VALIDATED = "Дата первой валидации ссылки"
    VALIDATION_ROW = "Строка валидации"
    SCORE = "БАЛЛ"
    CLIENT_PRICE = "Цена клиента"
    SOURCE_PRICE = "Цена на сайте"
    PRICE_DEVIATION = "% отклонения"
    CLIENT_IMG = "Ссылка на изображение товара клиента"
    SOURCE_IMG = "Ссылка на изображение товара на сайте"
    CLIENT_NAME = "Название товара клиента"
    SOURCE_NAME = "Название товара на сайте"


class SEMANTIC(NOTATION):
    """Semantic file notations"""

    NAME = "Название"
    SEARCH_QUERY = "Поисковый запрос"
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


VALIDATED = "validated"
