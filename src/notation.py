from abc import ABC


class NOTATION(ABC):
    pass


# columns from validation BrandPol
class VALIDATION(NOTATION):
    pass


class SEMANTIC(NOTATION):
    pass


VALIDATION_NAME = "Наименование"
VALIDATION_SOURCE = "Сайт"
VALIDATION_LINK = "Ссылка"
VALIDATION_REGION = "Регион"
VALIDATION_LINK_FROM = "Источник ссылки"
VALIDATION_DATE_FOUNDED = r"Дата первого\nобнаружения\nссылки"
VALIDATION_DATE_VALIDATED = "Дата первой валидации ссылки"
VALIDATION_ROW = "Строка валидации"
VALIDATION_SCORE = "БАЛЛ"
VALIDATION_CLIENT_PRICE = "Цена клиента"
VALIDATION_SOURCE_PRICE = "Цена на сайте"
VALIDATION_PRICE_DEVIATION = "% отклонения"
VALIDATION_CLIENT_IMG = "Ссылка на изображение товара клиента"
VALIDATION_SOURCE_IMG = "Ссылка на изображение товара на сайте"
VALIDATION_CLIENT_NAME = "Название товара клиента"
VALIDATION_SOURCE_NAME = "Название товара на сайте"

# columns from semantic BrandPol
SEMANTIC_NAME = "Название"
SEMANTIC_SEARCH_QUERY = "Поисковый запрос"
SEMANTIC_PLUS = "Плюс-слова"
SEMANTIC_MINUS = "Минус-слова"
SEMANTIC_REGEX = "Regex"
SEMANTIC_NOTE = "Note"
SEMANTIC_BARCODE = "Штрихкод"
SEMANTIC_BRAND = "Brand"
SEMANTIC_CATEGORY1 = "Категория"
SEMANTIC_CATEGORY2 = "Категория 2"
SEMANTIC_CLIENT_NAME = "Название клиента"
SEMANTIC_CLIENT_IMG = "Ссылка на фото"

VENDOR_CODE = "vendor_code"
VALIDATED = "validated"
VC_STATUS = "Валидация по артикулу"
NUMERICAL_FEATURES = "numerical_features"
STRING_FEATURES = "string_features"

CLIENT_FEATURES = "client_features"
SOURCE_FEATURES = "source_features"
CIF = "CLIENT_INTERMEDIATE_FEATURES"
SIF = "SOURCE_INTERMEDIATE_FEATURES"

TF_STATUS = "Валидация по текстовым признакам"
CLIENT_DESICIVE_FEATURES = "client_desicive_features"
SOURCE_DESICIVE_FEATURES = "source_desicive_features"
INTERMEDIATE_VALIDATION = "intermediate_validation"
