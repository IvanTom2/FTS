import regex as re
from collections import Counter

import re

string1 = r"n1"
string2 = r"№1"
string3 = r"n10"
string4 = r"№ 10"
string5 = r"n100"
string6 = r"№ 100"
string7 = r"n7"
string8 = r"№ 7"


# rx = r"([2-9][,.]?\d*|[1-9]\d+[,.]?\d*)\s*(?:шт|уп|пач|доз)"
rx = r"(?:n|№|x|х)\s*([2-9][,.]?\d*|[1-9]\d+[,.]?\d*)"


print(re.findall(rx, string1))
print(re.findall(rx, string2))
print(re.findall(rx, string3))
print(re.findall(rx, string4))
print(re.findall(rx, string5))
print(re.findall(rx, string6))
print(re.findall(rx, string7))
print(re.findall(rx, string8))
