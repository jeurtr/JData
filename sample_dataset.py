#-*- coding: utf-8 -*-

ACTION_201602_FILE = "JData_Action_201602.csv"
ACTION_201603_FILE = "JData_Action_201603.csv"
ACTION_201603_EXTRA_FILE = "JData_Action_201603_extra.csv"
ACTION_201604_FILE = "JData_Action_201604.csv"
NEW_USER_FILE = "JData_User_New.csv"
COMMENT_FILE = "JData_Comment.csv"

file_list = [ACTION_201602_FILE, ACTION_201603_FILE,
             ACTION_201603_EXTRA_FILE, ACTION_201604_FILE,
             NEW_USER_FILE, COMMENT_FILE]

for fname in file_list:
    with open("data_ori/" + fname, 'rb') as fi:
        with open('data/' + fname, 'wb') as fo:
            for i in range(30000):
                fo.write(fi.readline())
