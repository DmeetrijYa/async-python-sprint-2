import datetime

from job import Job
from logger import logger
from scheduler import Scheduler
from tasks import (multy_job_func,
                   web_job_func,
                   fs_job_func)
from tests import testing

if __name__ == '__main__':
    testing()
    scheduler = Scheduler(pool_size=10)
    multy_job = Job(multy_job_func, args=((1, 2), (10, 20)), start_at=datetime.datetime(2023, 7, 26, 20, 56),
                    max_working_time=60, tries=10, dependencies=[])
    web_job = Job(web_job_func, args=(('http://ya.ru', 'http://google.ru', 'http://yandex.ru')),
                  start_at=datetime.datetime(2023, 7, 25, 22, 30), max_working_time=120, tries=3,
                  dependencies=[multy_job])
    fs_job = Job(fs_job_func, args=(('./files'), ('./files/new_dir')), max_working_time=180, tries=5,
                 dependencies=[web_job])
    try:
        scheduler.put_tasks(multy_job)
        scheduler.put_tasks(web_job)
        scheduler.put_tasks(fs_job)
        scheduler.run()
    except (ValueError, IndexError) as e:
        logger.info(e)
