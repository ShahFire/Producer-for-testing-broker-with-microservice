# -*- coding: cp1251 -*-
import random
import string


def generate_random_string(length):
    signs = string.ascii_letters + string.digits
    line = ''.join(random.sample(signs, length))
    return line
