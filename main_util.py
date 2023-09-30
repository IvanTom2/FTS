from pathlib import Path


class TEST_DATA(object):
    PATH = Path(__file__).parent.parent / Path("Data")

    GROCERY = PATH / Path("Grocery")
    FARMACY = PATH / Path("Farmacy")
    BULGARIAN = PATH / Path("Bulgarian")

    VKUSVILL1 = GROCERY / Path("VkusVill1.xlsx")
    VKUSVILL2 = GROCERY / Path("VkusVill2.xlsx")

    FARMAIMPEX1 = FARMACY / Path("FarmaImpex1.xlsx")
    FARMAIMPEX2 = FARMACY / Path("FarmaImpex2.xlsx")


if __name__ == "__main__":
    print(TEST_DATA.PATH)
