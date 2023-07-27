import enum
import pathlib

import requests
from requests import Response


class TaskStatus(str, enum.Enum):
    READY = 'ready'
    RUNNING = 'running'
    COMPLETED = 'completed'
    ON_TIME = 'on_time'
    DEPENDENT = 'dependent'


def multy_job_func(*args) -> int:
    return sum(*args)


def web_job_func(url: str = 'https://ya.ru') -> Response | str:
    resp = requests.get(url=url)
    try:
        return resp
    except Exception:
        return "Ошибка выполнения запроса"


def fs_job_func(dir: str, pattern: str = '*.*') -> str:
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
