import sys

from flask import Flask
from flask_restful import Api, Resource, reqparse
import pika

app = Flask(__name__)
api = Api(app)


class Task:
    def __init__(self, id_, url):
        self.id = id_
        self.status = 'waiting'
        self.url = url
        self.result = None

    def finish(self, result: str):
        self.status = 'finished'
        self.result = result


class TaskResource(Resource):
    def __init__(self):
        super().__init__()

        self.conn_to_parser = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        self.ch_to_parser = self.conn_to_parser.channel()
        self.ch_to_parser.queue_declare(queue='task_queue', durable=True)

        self.conn_from_parser = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5001)
        )
        self.ch_from_parser = self.conn_from_parser.channel()
        self.ch_from_parser.queue_declare(queue='results_queue', durable=True)
        self.ch_from_parser.basic_qos(prefetch_count=1)
        self.ch_from_parser.basic_consume(self.update_task, queue='results_queue')

        self.tasks_list = dict()

        self.req_parser = reqparse.RequestParser()
        self.req_parser.add_argument("url")

    def update_task(self, ch, method, properties, task: str):
        task = task.split(';')
        id_ = int(task[0])
        self.tasks_list[id_].finish(task[1])
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def get(self, id_):
        if id_ in self.tasks_list:
            if self.tasks_list[id_].status != 'finished':
                return self.tasks_list[id_].status, 200
            else:
                return self.tasks_list[id_].result, 200
        return 'Task not found', 404

    def post(self):
        url = self.req_parser.parse_args()
        id_ = len(self.tasks_list)
        self.tasks_list[id_] = Task(id_, url)
        self.ch_to_parser.basic_publish(exchange='',
                                        routing_key='task_queue',
                                        body=f'{id_};{url}',
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                        ))
        return str(id_), 201

    def __del__(self):
        self.connection.close()


api.add_resource(TaskResource, '/<int:id_>', '/')

if __name__ == '__main__':
    app.run(debug=True)
