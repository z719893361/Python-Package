import contextlib
import queue
import threading


class ThreadPool:
    def __init__(self, thread_max_num, max_task_num=0):
        """
        :param  thread_max_num:         最大线程数
        :param  max_task_num:           最大队列数
        :var    self.queue:             任务队列
        :var    self.thread_max_num:    线程最大数
        :var    self.free_list:         空闲线程
        :var    self.generate_list:     已实例化的线程
        :return:
        """
        self.queue = queue.Queue(max_task_num)
        # 设置线程池最多可实例化的线程数
        self.thread_max_num = thread_max_num
        # 已实例化的线程列表
        self.generate_list = []
        # 处于空闲状态的线程列表
        self.free_list = []
        # 任务取消标识
        self.__cancel = False
        # 任务中断标识
        self.__interrupt = False

    def put(self, func, kwargs=None, callback=None):   # 往任务队列加入一个任务
        """
        :param func:        方法对象
        :param kwargs:      传入参数
        :param callback:    回调函数(success, result)
        :return:
        """
        # 判断任务是否已经取消
        if self.__cancel:
            return
        # 如果没有空闲的线程, 并且已经创建的线程数量小于预设的线程数量, 则创建新线程
        if len(self.free_list) == 0 and len(self.generate_list) < self.thread_max_num:
            threading.Thread(target=self.__call).start()
        # 构造任务元组, 分别是 方法, 参数, 回调方法
        w = (func, kwargs, callback)
        # 将任务添加到任务队列
        self.queue.put(w)

    def __call(self):
        # 获取当前线程名
        current_thread = threading.currentThread().getName()
        # 获取线程名, 将线程名加入到已经实例化线程列表中
        self.generate_list.append(current_thread)
        # 从任务队列中获取一个任务
        event = self.queue.get()

        # 获取的任务不是终止线程标识对象时
        while event is not None:
            # 解析任务重封装的三个参数
            func, kwargs, callback = event
            try:
                # 正常执行任务函数
                if kwargs is None:
                    result = func()
                else:
                    result = func(**kwargs)
                success = True
            except Exception as error:
                # 当任务执行过程中弹出异常
                result = None
                success = False
                print('执行任务方法失败 error: %s' % error)
            # 如果有指定的回调函数
            if callback is not None:
                # 执行回调函数,并抓取异常
                try:
                    callback(success, result)
                except Exception as error:
                    print(str('执行回调函数出错 %s' % error))
            self.queue.task_done()
            with self.__worker_state(self.free_list, current_thread):
                if self.__interrupt:
                    # event等于None 跳出循环
                    event = None
                # 否则获取一个正常的任务, 并回调worker_state方法的yield语句
                else:
                    # 获取新的任务继续循环
                    event = self.queue.get()
        else:
            self.generate_list.remove(current_thread)

    # 上下文管理,放入
    @contextlib.contextmanager
    def __worker_state(self, state_list, worker_thread):
        try:
            state_list.append(worker_thread)
            yield
        except Exception as error:
            print('worker_state error: %s' % error)
        finally:
            state_list.remove(worker_thread)

    def close(self):
        """
        不在添加任务, 执行完成即退出
        """
        self.__cancel = True
        # 获取已创建的线程个数, 往任务队列里推送数量相同的标识元素
        while self.generate_list:
            self.queue.put(None)

    def quit(self):
        """
        强制退出
        """
        self.__interrupt = True
        # 获取已创建的线程个数, 往任务队列里推送数量相同的标识元素
        while self.generate_list:
            self.queue.put(None)
            
