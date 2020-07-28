from threading import Thread

from flask import Flask
from flask_restful import Api, Resource, reqparse
from parser import Parser

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


class TaskResource(Resource):

    def __init__(self):
        super().__init__()

        self.req_parser = reqparse.RequestParser()
        self.req_parser.add_argument("url")

    def get(self, id_):
        if id_ < global_id:
            if tasks_list[id_].status != 'finished':
                return tasks_list[id_].status, 200
            else:
                return tasks_list[id_].result, 200
        return 'Task not found', 404

    def post(self):
        global global_id
        url = self.req_parser.parse_args()
        id_ = global_id
        global_id += 1
        task = Task(id_, url)
        todo_list.append(task)
        tasks_list.append(task)
        return str(id_), 201


api.add_resource(TaskResource, '/<int:id_>', '/')
global_id = 0

tasks_list = []
todo_list = []
done_list = []
parser = Parser()
parser_proc = Thread(target=parser.run, args=(todo_list, done_list))
parser_proc.daemon = True
parser_proc.start()


if __name__ == '__main__':
    app.run(debug=True)
