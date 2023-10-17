import pandas as pd
import numpy as np
import math


if __name__ == "__main__":
    df = pd.DataFrame(data=[1, 2, 3, 4, 5], columns=["a"])

    slices = math.ceil(len(df) / 3)

    frames = np.array_split(df, 3)
    print(frames)
