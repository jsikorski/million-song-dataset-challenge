import time


def invoke_measurable_task(task, task_name):
    print 'Invoking task:', task_name
    start = time.time()
    task()
    print 'Task invoked in:', time.time() - start