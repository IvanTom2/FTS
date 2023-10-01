import regex as re
from collections import Counter

# import re

# string1 = r"n1"
# string2 = r"№1"

# rx = r"(?:n|№|x|х)\s*(?!1|(\.0)?)(\d*[,.]?\d+\s*шт)"
# # rx = r"(?:n|№|x|х)\s*\d*[.,]?\d+"

# print(re.findall(rx, string1))
# print(re.findall(rx, string2))

lst = ["a", "a", "b", "c", "d"]
counts = Counter(lst)

# print(counts.most_common()[0][1])


def parab(value):
    value = -4 * value**2 + 4 * value
    return value


print(parab(0.2))
