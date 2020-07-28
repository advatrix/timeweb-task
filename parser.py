import tarfile
import os

from bs4 import BeautifulSoup
from urllib import request
from pathlib import Path
from urllib.error import URLError


class Parser:
    def __init__(self):
        pass

    def run(self, todo_list, done_list):
        while True:
            if todo_list:
                task = todo_list.pop()

                Path('./'+str(task.id)).mkdir(parents=True, exist_ok=True)

                task.take()

                task.finish(self.parse(task.url['url'], str(task.id)))
                done_list.append(task)

    def parse(self, url, dirname, depth=3):
        try:
            response = request.urlopen(url).read()
        except (ValueError, URLError) as e:
            return

        soup = BeautifulSoup(response, features='html.parser')

        links = [a.get('href') for a in soup.find_all('a', True)]
        images = [img.get('src') for img in soup.find_all('img')]
        scripts = [script.get('src') for script in soup.find_all('script')]
        styles = [link.get('href') for link in soup.find_all('link') if 'stylesheet' in link.get('rel')]
        inline_styles = soup.find_all('style')

        with open(dirname+'/inline_styles', 'w') as fout:
            for style in inline_styles:
                fout.write(style.contents[0])

        for resource in images + styles + scripts:
            if resource:
                try:
                    resource_response = request.urlopen(resource).read()
                    with open(dirname+'/'+process(resource), 'wb') as fout:
                        fout.write(resource_response)
                except (ValueError, URLError):
                    pass

        with open(dirname+'/'+process(url), 'wb') as fout:
            fout.write(response)

        if depth != 1:
            for link in links:
                self.parse(link, dirname, depth-1)

        with tarfile.open(dirname+'.tgz', 'w:gz') as tar:
            for name in os.listdir(dirname):
                tar.add(dirname + '/' + name)

        return '/' + dirname + '.tgz'


def process(url):
    return str(url).replace('/', '_').replace(':', '_')
