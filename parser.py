from bs4 import BeautifulSoup
from urllib import request
from urllib.error import URLError
import os
import pika


class Parser:
    def __init__(self):
        self.conn_from_api = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        self.ch_from_api = self.conn_from_api.channel()
        self.ch_from_api.queue_declare(queue='task_queue', durable=True)
        self.ch_from_api.basic_qos(prefetch_count=1)
        self.ch_from_api.basic_consume(self.take_task, queue='hello')
        self.ch_from_api.start_consuming()

        self.conn_to_api = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5001)
        )
        self.ch_to_api = self.conn_to_api.channel()
        self.ch_to_api.queue_declare(queue='results_queue', durable=True)

    def take_task(self, ch, method, properties, task):
        id_, url = task.split(';')
        result = self.parse(url)
        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.ch_to_api.basic_publish(
            exchange='',
            routing_key='results_queue',
            body=f'{id_};{result}',
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )

    def parse(self, url, depth=3) -> str:
        try:
            response = request.urlopen(url).read()
        except URLError:
            return ''
        soup = BeautifulSoup(response)
        print(soup)

    def __del__(self):
        self.conn_from_api.close()
