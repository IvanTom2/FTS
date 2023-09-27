import regex as re

# import re

string1 = r"n1"
string2 = r"№1"

rx = r"(?:n|№|x|х)\s*(?!1|(\.0)?)(\d*[,.]?\d+\s*шт)"
# rx = r"(?:n|№|x|х)\s*\d*[.,]?\d+"

print(re.findall(rx, string1))
print(re.findall(rx, string2))
