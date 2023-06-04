# -*- coding: cp1251 -*-
# ����������� ����������� ������ � �������
import configparser
import json
import requests
import numpy as np

from src.brokers import *
from src.databases import *
from src.generators import *
from src.times import *


# �������� �������, ������� ����� ����������� ��� ���������� ���������
def execute_main():
    # ������� ��������� ������� ConfigParser � ��������� ���������������� ���� 'config.properties'
    config = configparser.ConfigParser()
    config.read('config.properties')

    # �������� �������� ���������� ��� ����������� � RabbitMQ �� ����������������� �����
    cfg_mq_ip = config.get('mq', 'mq_ip')

    # �������� �������� ���������� ��� ����������� � �� �� ����������������� �����
    cfg_db_ip = config.get('database', 'db_ip')
    cfg_db_name = config.get('database', 'db_name')
    cfg_db_user = config.get('database', 'db_user')
    cfg_db_password = config.get('database', 'db_password')
    cfg_db_port = config.get('database', 'db_port')
    
    duration = int(config.get('main', 'duration'))
    rate1 = int(config.get('main', 'rate1'))
    rate2 = int(config.get('main', 'rate2'))

    micro_url = config.get('microservice', 'micro_url')

    payload = {'queue': 'mail_warehouse'}
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print(response.content)

    # ������������� ���������� � RabbitMQ � ������� �� ������ �������� 'mail' ���� 'fanout' � ������ 'mail_warehouse'
    mq_connection = mq_connect(cfg_mq_ip)
    mq_channel = mq_connect_channel(mq_connection)
    mq_create_exchange(mq_channel, 'mail', 'fanout', 'mail_warehouse', '*')

    # ������� ���������� � �� � ������� ������ ������
    my_connection = db_create_connection(cfg_db_name, cfg_db_user, cfg_db_password, cfg_db_ip, cfg_db_port)
    delete_data(my_connection)

    arr = np.round(np.linspace(rate1, rate2, duration))
    print(arr)

    total_sum = int(arr.sum())
    print(total_sum)

    dt_id = 1
    start_time = get_datetime()
    iterations = 0

    one_second = datetime.timedelta(seconds=1)

    insert_data(my_connection, total_sum)

    while iterations < duration:
        current_time = get_datetime()

        if (current_time - start_time) >= one_second:
            start_time += one_second
            # ���������� � ���������� ��������� � RabbitMQ
            for i in range(1, (int(arr[iterations]))):
                body = {
                    "id": dt_id,
                }
                json_body = json.dumps(body)
                mq_publish_message(mq_channel, 'mail', '*', json_body)

                dt_id += 1

            iterations += 1

    # ��������� ���������� � RabbitMQ
    mq_close_connect(mq_connection)

    # �������� � ������� ������ �� ��
    select_data(my_connection)


# ���������, ��� ������ ���� ����������� ��������, � �� ������������� ��� ������
if __name__ == '__main__':
    execute_main()
