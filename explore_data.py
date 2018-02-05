#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np

ACTION_201602_FILE = "data/JData_Action_201602.csv"
ACTION_201603_FILE = "data/JData_Action_201603.csv"
ACTION_201603_EXTRA_FILE = "data/JData_Action_201603_extra.csv"
ACTION_201604_FILE = "data/JData_Action_201604.csv"
COMMENT_FILE = "data/JData_Comment.csv"
PRODUCT_FILE = "data/JData_Product.csv"
USER_FILE = "data/JData_User.csv"
NEW_USER_FILE = "data/JData_User_New.csv"

# Display format
pd.options.display.float_format = '{:,.3f}'.format


def tranform_user_age():
    # Load data, header=0 means that the file has column names
    df = pd.read_csv(USER_FILE, header=0, encoding="gbk")

    for i in range(len(df['age'])):
        if df['age'][i] == u"15岁以下":
            df['age'][i] = 0
        elif df['age'][i] == u"16-25岁":
            df['age'][i] = 1
        elif df['age'][i] == u"26-35岁":
            df['age'][i] = 2
        elif df['age'][i] == u"36-45岁":
            df['age'][i] = 3
        elif df['age'][i] == u"46-55岁":
            df['age'][i] = 4
        elif df['age'][i] == u"56岁以上":
            df['age'][i] = 5
        else:
            df['age'][i] = -1

    df['user_reg_dt'] = pd.to_datetime(df['user_reg_dt'])
    min_date = min(df['user_reg_dt'])
    df['user_reg_diff'] = [int(i.days) for i in (df['user_reg_dt'] - min_date)]

    df.to_csv(NEW_USER_FILE, index=False)


def explore_user():
    df = pd.read_csv(NEW_USER_FILE, header=0)
    # Get first 5 rows, also you can use df.tail(10) to get last 10 rows
    print(df.head(5))
    # Basic statistical information
    print(df.describe())
    # Each column type
    print(df.dtypes)


def explore_action_02(chunk_size=100000):
    # Number of Record: 18117303
    reader = pd.read_csv(ACTION_201602_FILE, header=0, iterator=True)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df = pd.concat(chunks, ignore_index=True)
    print(df.head(5))
    print(df.dtypes)

    print(df[df["user_id"] == 27630])

if __name__ == "__main__":
    # 进行年龄映射
    tranform_user_age()

    # explore_user()
    explore_action_02()
