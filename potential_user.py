#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np

ACTION_201602_FILE = "data_ori/JData_Action_201602.csv"
ACTION_201603_FILE = "data_ori/JData_Action_201603.csv"
ACTION_201603_EXTRA_FILE = "data_ori/JData_Action_201603_extra.csv"
ACTION_201604_FILE = "data_ori/JData_Action_201604.csv"
COMMENT_FILE = "data/JData_Comment.csv"
PRODUCT_FILE = "data/JData_Product.csv"
USER_FILE = "data/JData_User.csv"
NEW_USER_FILE = "data/JData_User_New.csv"
USER_TABLE_FILE = "data/user_table.csv"
BUY_USER_LIST_FILE = "data/buy_user_list.csv"
PROTENTIAL_USER_RECORD = "data/protential_user_record.csv"


def ui_record_in_batch_data(fname, ui_pair, chunk_size=100000):
    reader = pd.read_csv(fname, header=0, iterator=True)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)[
                ["user_id", "sku_id", "time", "type"]]
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df_ac = pd.concat(chunks, ignore_index=True)

    df = []
    for index, row in ui_pair.iterrows():
        usr_id = row["user_id"]
        sku_id = row["sku_id"]

        # find U-I related record
        df.append(df_ac[(df_ac["user_id"] == usr_id) &
                        (df_ac["sku_id"] == sku_id)])

    df = pd.concat(df, ignore_index=True)

    return df


def more_than_a_day(group):

    last_buy_day = max(group[group["type"] == 4]["date"])
    earliest_behave_day = min(group["date"])

    if (last_buy_day - earliest_behave_day).days > 0:
        group["potential_flag"] = 1
    else:
        group["potential_flag"] = 0

    return group


def find_potential_user():

    ui_pair = pd.read_csv(BUY_USER_LIST_FILE, header=0)

    # ui_pair = ui_pair.head(5)

    df_ac = []
    df_ac.append(ui_record_in_batch_data(ACTION_201602_FILE, ui_pair))
    df_ac.append(ui_record_in_batch_data(ACTION_201603_FILE, ui_pair))
    df_ac.append(ui_record_in_batch_data(ACTION_201603_EXTRA_FILE, ui_pair))
    df_ac.append(ui_record_in_batch_data(ACTION_201604_FILE, ui_pair))

    df_ac = pd.concat(df_ac, ignore_index=True)
    df_ac = df_ac.drop_duplicates()

    # df_ac = potential_user_in_batch_data(ACTION_201602_FILE, ui_pair)

    df_ac['date'] = pd.to_datetime(df_ac['time']).dt.date

    df_ac = df_ac.groupby(["user_id", "sku_id"]).apply(more_than_a_day)

    df_ac = df_ac[df_ac["potential_flag"] == 1]

    df_ac.to_csv(PROTENTIAL_USER_RECORD, index=False)


def buy_user_in_batch_data(fname, chunk_size=100000):
    reader = pd.read_csv(fname, header=0, iterator=True)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)[
                ["user_id", "sku_id", "type"]]
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df_ac = pd.concat(chunks, ignore_index=True)

    # find buy record
    df_ac = df_ac[df_ac['type'] == 4][["user_id", "sku_id"]]

    return df_ac


def find_buy_user():
    df_ac = []
    df_ac.append(buy_user_in_batch_data(fname=ACTION_201602_FILE))
    df_ac.append(buy_user_in_batch_data(fname=ACTION_201603_FILE))
    df_ac.append(buy_user_in_batch_data(fname=ACTION_201603_EXTRA_FILE))
    df_ac.append(buy_user_in_batch_data(fname=ACTION_201604_FILE))

    df_ac = pd.concat(df_ac, ignore_index=True)
    df_ac = df_ac.drop_duplicates()

    df_ac.to_csv(BUY_USER_LIST_FILE, index=False)


find_buy_user()
find_potential_user()
