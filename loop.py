import datetime
import json
import asyncio
import os
import pathlib

import aiohttp
from aiohttp import ClientResponse
from requests import Response

from job import Job
from scheduler import Scheduler
from collections import deque
import requests

jobs = deque()
def loop():
    scheduler = Scheduler(pool_size=10)
    # multy-job
    job_1 = Job(test_func_1, args=((1, 2), (10, 20)), start_at= datetime.datetime(2023, 7, 26, 20, 56), max_working_time=60, tries=10, dependencies=[])
    # web-job
    job_2 = Job(test_func_2, args=(('http://ya.ru', 'http://google.ru', 'http://yandex.ru')), start_at= datetime.datetime(2023, 7, 25, 22, 30), max_working_time=120, tries=3, dependencies=[job_1])
    # fs-job
    job_3 = Job(test_func_3, args=(('./files'), ('./files/new_dir')), max_working_time=180, tries=5, dependencies=[job_2])
    jobs.append(job_1)
    jobs.append(job_2)
    jobs.append(job_3)
    scheduler.put_tasks(jobs)
    scheduler.run()

def test_func_1(*args) -> int:
    return sum(*args)


def test_func_2(url: str = 'https://ya.ru') -> Response | str:
        resp = requests.get(url=url)
        try:
            return resp
        except Exception:
            return "Ошибка выполнения запроса"
def test_func_3(dir: str, pattern: str = '*.*'):
    # print(os.getcwd())
    def get_paths(topdir, pattern):
        for path in pathlib.Path(topdir).rglob(pattern):
            if path.exists():
                yield path

    def get_files(paths):
        for path in paths:
            with path.open('rt', encoding='utf-8') as file:
                yield file

    def get_lines(files):
        for file in files:
            yield from file

    paths = get_paths(dir, '*.*')
    files = get_files(paths)
    lines = get_lines(files)
    result = ''
    for line in lines:
        result += line
    return result


if __name__ == '__main__':
    # lines = ''
    # for line in test_func_3("./files"):
    #     lines += line
    # print(len(lines))
    loop()
