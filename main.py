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

    def take(self):
        self.status = 'processing'

    def finish(self, result: str):
        self.status = 'finished'
        self.result = result

    def __len__(self):
        return sys.getsizeof(self)


class TaskResource(Resource):
    def __init__(self):
        super().__init__()
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )

        self.channel = self.connection.channel()

        self.channel.queue_declare(queue='task_queue', durable=True)
        self.tasks_list = dict()

        self.parser = reqparse.RequestParser()
        self.parser.add_argument("url")

    def get(self, id_):
        if id_ in self.tasks_list:
            if self.tasks_list[id_].status != 'finished':
                return self.tasks_list[id_].status, 200
            else:
                return self.tasks_list[id_].result, 200
        return 'Task not found', 404

    def post(self):
        url = self.parser.parse_args()
        id_ = len(self.tasks_list)
        task = Task(id_, url)
        self.tasks_list[id_] = task
        self.channel.basic_publish(exchange='',
                                   routing_key='task_queue',
                                   body=task,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2,
                                   ))
        return str(id_), 201

    def __del__(self):
        self.connection.close()


api.add_resource(TaskResource, '/<int:id_>', '/')

if __name__ == '__main__':
    app.run(debug=True)
