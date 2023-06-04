# -*- coding: cp1251 -*-
import pika


# ����������� � RabbitMQ
def mq_connect(ip):
    connect = pika.BlockingConnection(pika.ConnectionParameters(host=ip))
    return connect


# ����������� � RabbitMQ
def mq_connect_channel(connect):
    channel = connect.channel()
    return channel


def mq_create_exchange(channel, exchange, exchange_type, queue, routing_key):
    # �������� Exchange (���������)
    channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)

    # �������� �������
    my_queue = channel.queue_declare(queue=queue)

    # �������� (bind) ������� � ���������
    channel.queue_bind(exchange=exchange, queue=my_queue.method.queue, routing_key=routing_key)


# �������� ���������
def mq_publish_message(channel, exchange, routing_key, body):
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)


def mq_close_connect(connect):
    connect.close()
