import time
import threading


class TaskList:
    __timestamp__ = None
    __timeout__ = None
    __tasks_undo__ = None
    __tasks_pending__ = None
    __lock__ = None

    def __init__(self, timeout=5):
        self.__tasks_undo__ = list()
        self.__tasks_pending__ = list()
        self.__timeout__ = timeout
        self.__lock__ = threading.Lock()
        self.__update_timestamp__()

    def set_timeout(self, timeout):
        self.__lock__.acquire()
        self.__check_timeout__()
        self.__timeout__ = timeout
        self.__update_timestamp__()
        self.__lock__.release()

    def put_tasks(self, tasks):
        self.__lock__.acquire()
        self.__check_timeout__()
        self.__tasks_undo__.extend(tasks)
        self.__update_timestamp__()
        self.__lock__.release()

    def get_task(self):
        self.__lock__.acquire()
        self.__check_timeout__()
        if len(self.__tasks_undo__) == 0:
            return None
        task = self.__tasks_undo__.pop()
        self.__tasks_pending__.append(task)
        self.__update_timestamp__()
        self.__lock__.release()
        return task

    def done_task(self, task):
        self.__lock__.acquire()
        self.__check_timeout__()
        self.__tasks_pending__.remove(task)
        self.__update_timestamp__()
        self.__lock__.release()

    def is_empty(self):
        self.__lock__.acquire()
        self.__check_timeout__()
        ret = (len(self.__tasks_undo__) + len(self.__tasks_pending__) == 0)
        self.__lock__.release()
        return ret

    def __update_timestamp__(self):
        self.__timestamp__ = int(time.time())

    def __check_timeout__(self):
        cur_time = int(time.time())
        if cur_time > self.__timestamp__ + self.__timeout__:
            self.__tasks_undo__.extend(self.__tasks_pending__)
            self.__tasks_pending__.clear()
