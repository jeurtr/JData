#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np

USER_TABLE_FILE = "data/user_table.csv"
ITEM_TABLE_FILE = "data/item_table.csv"

df_usr = pd.read_csv(USER_TABLE_FILE, header=0)
# print(df_usr.head())

df_usr['buy_num'] != 0
