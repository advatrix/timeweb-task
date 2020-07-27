from bs4 import BeautifulSoup
from urllib import request
from urllib.error import URLError
import os
import pika


class Parser:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue='task_queue', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.take_task, queue='hello')
        self.channel.start_consuming()

    def take_task(self, ch, method, properties, task):
        task.take()
        task.finish(self.parse(task.url))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def parse(self, url, depth=3):
        try:
            response = request.urlopen(url).read()
        except URLError:
            return
        soup = BeautifulSoup(response)
        print(soup)

    def __del__(self):
        self.connection.close()
