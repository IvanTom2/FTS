import pandas as pd
import numpy as np
import math


if __name__ == "__main__":
    lst1 = list(map(set, [[1, 2], [2], [3]]))
    lst2 = list(map(set, [[1, 1], [2], [3]]))

    mapper = list(zip(lst1, lst2))
    print(mapper)
