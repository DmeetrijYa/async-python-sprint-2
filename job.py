import json
import logging
import time
import datetime
from functools import wraps

from urllib3 import HTTPSConnectionPool

_log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
logger = logging.getLogger('task_manager')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(filename=__file__ + '.log', )
handler.setFormatter(logging.Formatter(_log_format))
logger.addHandler(handler)

def coroutine(f):
    @wraps(f) # https://docs.python.org/3/library/functools.html#functools.wraps
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen
    return wrap

class Job:
    state: {}
    def __init__(self,
                 target=None,
                 args: tuple = None,
                 kwargs: dict = None,
                 start_at: datetime.datetime = None,
                 max_working_time: int = -1,
                 tries: int = 0,
                 dependencies: list = [],
                 state: dict = {}):
        self.target = target
        self.args = args or ()
        self.kwargs = kwargs or {}
        # self.coroutine = target(*self.args, **self.kwargs)
        self.start_at = start_at
        self.max_working_time = max_working_time,
        self.tries = tries,
        self.dependencies: list['Job'] = dependencies or []
        # self.data = data
        # self.state(state)
        self.state = state
        self.state['started_at'] = time.time()
        # self._uuid = uuid.uuid4()

    def put_job(self):
        raise NotImplemented

    def run(self):
        try:
            if isinstance(self.tries, tuple):
                self.tries = self.tries[0]
            elif isinstance(self.tries, int):
                pass
            else:
                raise ValueError
            if self.tries > 0 and (time.time() - self.state.get('started_at') <= self.max_working_time[0]):
                if self.start_at:
                    delay = (datetime.datetime.now() - self.start_at).total_seconds()
                    if delay < 0:
                        self.tries -= 1
                        yield 'on_time', delay, self.tries
                    else:
                        yield 'ready', None, self.tries
                elif self.dependencies:
                    if self.check_dependencies():
                        # self.tries -= 1
                        yield 'ready', None, self.tries
                        # self._run()
                    else:
                        self.tries -= 1
                        yield 'dependent', self.dependencies, self.tries
                else:
                    self._run()
            else:
                raise StopIteration
        except StopIteration as e:
            print(f"All attempts have been exhausted")

    @coroutine
    def _run(self):
        try:
            self.state['started_at'] = time.time()
            self.state['status'] = status = 'running'
            yield status, self.state
            if self.args:
                # print(f"{self.args = }")
                for arg in self.args:
                    # print(f"{arg = }")
                    self.state['result'] = result = self.target(arg)
                    self.state['status']  = status = 'running'
                    # print(f"{self.args = }")
                    self.args = (item for item in self.args if item != arg)
                    # print(f"{self.args = }")
                    print(f'Job is running, {self.state.get("result") = }')
                    logger.info(f'{self.target.__name__} Job completed successfully \n with result {result}')
                    yield status, result # , self.args
            elif self.kwargs:
                for key, value in self.kwargs.items():
                    self.state['result'] = result = self.target({key: value})
                    self.state['status'] = status = 'running'
                    self.kwargs.pop(key, None)
                    print(f'Job is running, {self.state.get("result") = }')
                    logger.info(f'{self.target.__name__} Job completed successfully \n with result {result}')
                    yield status, result # , self.kwargs

            self.state['status'] = status = 'completed'
            self.state['completed'] = True
            self.state['end_at'] = time.time()
            yield status, self.state
        except Exception as e:
            print("Task done", e)

    def pause(self):
        pass

    def stop(self):
        with open('tasks.inf', 'w') as file:
            json.dumps(self.state)

    def check_comlpetion(self):
        return self.state.get('completed')

    def check_dependencies(self):
        return all(task.check_comlpetion() for task in self.dependencies)
