import datetime
import json
import logging
from collections import deque

from job import Job

class Scheduler:
    def __init__(self, pool_size=10):
        self.tasks = deque(maxlen=pool_size)
        self.finished_tasks = []

    def put_tasks(self, tasks):
        for task in tasks:
            self.tasks.append(task)

    def schedule(self, task: Job):
        if datetime.datetime.now() == task.start_at:
            self.run()

    def tasks_dump(self):
        return json.dumps(self.state)

    def get_state(self):
        print(f"get_state: {json.dumps(self.state) = }")
        return json.dumps(self.state)

    def set_state(self, state):
        if self.state is None:
            self.state = {}
        else:
            self.state = json.loads(state)
        print(f"set_state: {self.state = }")

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
                        if cond == "on_time":
                            task.tries = tries
                            self.tasks.append(task)
                        elif cond == "dependent":
                            for completed_task in self.finished_tasks:
                                if completed_task in task.dependencies:
                                    task.dependencies.remove(completed_task)
                            task.tries = tries
                            self.tasks.append(task)
                        elif cond == 'running':
                            res = task._run()
                            status, result = next(res)
                            print(f"{status = }, {result = }")
                            self.tasks.append(task)
                        elif cond == 'completed':
                            print(f'Task is already completed')
                        elif cond == 'ready':
                            res = task._run()
                            status, result = next(res)
                            # print(f"{status = }, {result = }")
                            if status == 'completed':
                                self.finished_tasks.append(task)
                            elif status == 'running':
                                self.tasks.append(task)
                    except StopIteration as e:
                        print(e)
            except StopIteration as e:
                print(e)

    def restart(self):
        self.stop()
        self.run()

    def stop(self):
        with open('tasks_suspended.inf', 'w') as file:
            file.write(self.tasks_dump())
