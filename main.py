import gdeltdoc
import pandas as pd
import numpy as np
from gdeltdoc import GdeltDoc, Filters


f = Filters(start_date="2022-01-01", end_date="2026-06-30", keyword="Taiwan")

gd = GdeltDoc()

data_art = gd.timeline_search("timelinetone", f)

print(data_art.head())


data_art.to_csv(r"./data")
