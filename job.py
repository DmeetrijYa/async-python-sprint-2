import datetime
import json
import time
from functools import wraps

from logger import logger
from tasks import TaskStatus

def coroutine(f):
    @wraps(f)  # https://docs.python.org/3/library/functools.html#functools.wraps
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
                 max_working_time: int = None,
                 tries: int = None,
                 dependencies: list = None,
                 state: dict = None):
        self.target = target
        self.tries = tries
        self.args = args
        if kwargs is None:
            self.kwargs: dict = {}
        else:
            self.kwargs: dict = kwargs
        if start_at is None:
            self.start_at = datetime.datetime.now()
        else:
            self.start_at = start_at
        self.max_working_time = max_working_time
        if dependencies is None:
            self.dependencies: list['Job'] = []
        else:
            self.dependencies: list['Job'] = dependencies
        if state is None:
            self.state = {}
        else:
            self.state = state

        self.state['started_at'] = time.time()

    def put_job(self):
        raise NotImplemented

    def run(self):
        try:
            if isinstance(self.tries, int):
                pass
            else:
                raise ValueError('Tries must integer number')
            if self.tries > 0 and (time.time() - self.state.get('started_at') <= self.max_working_time):
                if self.start_at:
                    delay = (datetime.datetime.now() - self.start_at).total_seconds()
                    if delay < 0:
                        self.tries -= 1
                        yield TaskStatus.ON_TIME, delay, self.tries
                    else:
                        yield TaskStatus.READY, None, self.tries
                elif self.dependencies:
                    if self.check_dependencies():
                        yield TaskStatus.READY, None, self.tries
                    else:
                        self.tries -= 1
                        yield TaskStatus.DEPENDENT, self.dependencies, self.tries
                else:
                    self._run()
            else:
                raise StopIteration
        except StopIteration:
            logger.info("All attempts have been exhausted")

    @coroutine
    def _run(self):
        try:
            self.state['started_at'] = time.time()
            self.state['status'] = status = TaskStatus.RUNNING
            yield status, self.state
            if self.args:
                for arg in self.args:
                    self.state['result'] = result = self.target(arg)
                    self.state['status'] = status = TaskStatus.RUNNING
                    self.args = (item for item in self.args if item != arg)
                    logger.info(f'{self.target.__name__} Job completed successfully \n with result {result}')
                    yield status, result
            elif self.kwargs:
                for key, value in self.kwargs.items():
                    self.state['result'] = result = self.target({key: value})
                    self.state['status'] = status = TaskStatus.RUNNING
                    self.kwargs.pop(key, None)
                    logger.info(f'{self.target.__name__} Job completed successfully \n with result {result}')
                    yield status, result

            self.state['status'] = status = TaskStatus.COMPLETED
            self.state['completed'] = True
            self.state['end_at'] = time.time()
            yield status, self.state
        except StopIteration as e:
            logger.info(e)

    def pause(self):
        pass

    def stop(self):
        with open('tasks.inf', 'w') as file:
            json.dump(self.state, file)

    def check_completion(self):
        return self.state.get('completed')

    def check_dependencies(self):
        return all(task.check_completion() for task in self.dependencies)
