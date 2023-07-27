from job import Job
from logger import logger
from scheduler import Scheduler
from tasks import (multy_job_func, web_job_func, fs_job_func)


def testing():
    def test_multy_job_func():
        assert multy_job_func(range(5)) == 10
        assert multy_job_func((5, 10)) != 16
        assert isinstance(multy_job_func((5, 5)), int)
        logger.debug(f'Test for multy_job_func completed')

    def test_web_job_func():
        assert 200 <= int(web_job_func().status_code) < 300
        assert isinstance(web_job_func().status_code, int)
        logger.debug(f'Test for web_job_func completed')

    def test_fs_job_func():
        assert isinstance(fs_job_func('./files'), str)
        logger.debug(f'Test for fs_job_func completed')

    def test_job():
        test_job = Job(multy_job_func, args=(range(5),),
                       max_working_time=120, tries=10, dependencies=[])
        assert test_job.args == (range(5),)
        _, _, result = next(test_job.run())
        assert result == 10
        logger.debug('Test for class Job completed')

    def test_scheduler():
        scheduler = Scheduler(pool_size=1)
        assert scheduler.pool_size == 1
        test_job = Job(multy_job_func, args=(range(5),),
                       max_working_time=120, tries=10, dependencies=[])
        scheduler.put_tasks(test_job)
        scheduler.run()
        assert test_job.state.get('result') == 10
        logger.debug('Test for class Scheduler completed')

    test_multy_job_func()
    test_web_job_func()
    test_fs_job_func()
    test_job()
    test_scheduler()

    logger.info("All tests completed successfully!")


if __name__ == '__main__':
    testing()

