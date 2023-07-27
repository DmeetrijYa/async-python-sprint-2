import datetime
import json
from collections import deque

from job import Job
from logger import logger
from tasks import TaskStatus


class Scheduler:
    def __init__(self, pool_size: int = None):
        if pool_size is None:
            self.pool_size = 10
        else:
            self.pool_size = pool_size
        self.tasks = deque(maxlen=self.pool_size)
        self.finished_tasks = []

    def put_tasks(self, tasks):
        logger.debug(f'Scheduler.put_tasks called, input tasks:\n{tasks}')
        if isinstance(tasks, Job):
            if len(self.tasks) < self.pool_size:
                self.tasks.append(tasks)
            else:
                raise IndexError('Scheduler input deque is full')
        elif isinstance(tasks, list) or isinstance(tasks, deque):
            for task in tasks:
                if len(self.tasks) < self.pool_size:
                    self.tasks.append(task)
                else:
                    raise IndexError('Scheduler input deque is full')
        else:
            raise ValueError(
                f'Scheduler cannot put {tasks.__class__} into queue.\nTask must be Job object or it can be a list of Jobs or a collections.deque of Jobs')

    def schedule(self, task: Job):
        if datetime.datetime.now() == task.start_at:
            self.run()
            logger.info('Scheduler started on time job')

    def tasks_dump(self):
        return json.dumps(self.state)

    def get_state(self):
        logger.info(f"get_state: {json.dumps(self.state) = }")
        return json.dumps(self.state)

    def set_state(self, state):
        if self.state is None:
            self.state = {}
        else:
            self.state = json.loads(state)
        logger.info(f"set_state: {self.state = }")

    def reset_state(self):
        self.state = {}

    def run(self):
        while self.tasks:
            try:
                task = self.tasks.popleft()
                res = task.run()
                cond, job, tries = next(res)
                if tries:
                    try:
                        if cond == TaskStatus.ON_TIME:
                            task.tries = tries
                            self.tasks.append(task)
                        elif cond == TaskStatus.DEPENDENT:
                            for completed_task in self.finished_tasks:
                                if completed_task in task.dependencies:
                                    task.dependencies.remove(completed_task)
                            task.tries = tries
                            self.tasks.append(task)
                        elif cond == TaskStatus.RUNNING:
                            res = task._run()
                            status, result = next(res)
                            logger.debug(f'Job returns status: {status} and result: {result}')
                            self.tasks.append(task)
                        elif cond == TaskStatus.COMPLETED:
                            logger.info(f'Task: {task} with status:{TaskStatus.COMPLETED}\nTask is already completed')
                        elif cond == TaskStatus.READY:
                            res = task._run()
                            status, result = next(res)
                            if status == TaskStatus.COMPLETED:
                                self.finished_tasks.append(task)
                            elif status == TaskStatus.RUNNING:
                                self.tasks.append(task)
                    except StopIteration as e:
                        logger.info(e)
            except StopIteration as e:
                logger.info(e)

    def restart(self):
        self.stop()
        self.run()

    def stop(self):
        with open('tasks_suspended.inf', 'w') as file:
            file.write(self.tasks_dump())

