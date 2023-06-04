# -*- coding: cp1251 -*-
import datetime


def get_datetime():
    return datetime.datetime.now()


def get_time():
    return get_datetime().time()



