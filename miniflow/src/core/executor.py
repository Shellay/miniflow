from queue import Queue
from threading import Thread
from threading import RLock
# from time import sleep
# from uuid import uuid4

import logging
# import pprint

from canvas import (
    Task, Single, Group,
    AbstractChain, Chain, End
)


class Result:

    def __init__(self, task, value):
        self.task = task
        self.value = value


class Service:

    def __init__(self, n_workers):

        self.single_queue = Queue()
        self.result_queue = Queue()

        FORMAT = '[%(levelname)s] %(asctime)-15s [%(threadName)s] %(message)s'
        logging.basicConfig(format=FORMAT, level='DEBUG')
        self.logger = logging.getLogger()

        self.workers = []
        for i in range(n_workers):
            worker = Thread(target=self.work, args=(), name=f'Worker-{i+1}')
            self.workers.append(worker)

        self.handler = Thread(target=self.handle, args=(), name=f'Handler')

        self.sync_lock = RLock()
        self.sync_trigger = {}

        self.call_next_lock = RLock()
        self.call_next = {}

    def start(self):
        for worker in self.workers:
            worker.start()
        self.handler.start()

    def submit_task(self, task: Task):

        def get_chain_end(chain: AbstractChain):
            if isinstance(chain, End):
                return chain
            elif isinstance(chain, Chain):
                return get_chain_end(chain.chain)
            else:
                raise

        if isinstance(task, End):
            end = task
            self.single_queue.put(end)
        elif isinstance(task, Single):
            single = task
            self.single_queue.put(single)
        else:
            assert isinstance(task, Group)
            group = task
            for chn in group.chains:
                assert isinstance(chn, Chain)
                end = get_chain_end(chn)
                with self.sync_lock:
                    self.sync_trigger[end] = group
            for chn in group.chains:
                self.submit_chain(chn)

    def submit_chain(self, chain: AbstractChain):
        if isinstance(chain, End):
            end = chain
            self.submit_task(end)
        else:
            assert isinstance(chain, Chain)
            task = chain.task
            assert isinstance(task, (Single, Group))
            downstream = chain.chain
            with self.call_next_lock:
                self.call_next[task] = downstream
            self.submit_task(task)

    def work(self):
        try:
            while True:
                # single_task can be either Single or End
                single_task = self.single_queue.get()
                try:
                    value = single_task.run()
                except Exception as e:
                    value = e
                result = Result(single_task, value)
                self.result_queue.put(result)
        except KeyboardInterrupt:
            pass

    def handle(self):
        while True:
            result = self.result_queue.get()
            if isinstance(result.task, End):
                # chain end indicates synchronization
                end = result.task
                self.logger.debug(f'Handling {end}')
                group = None
                with self.sync_lock:
                    if end in self.sync_trigger:
                        group = self.sync_trigger.pop(end)
                        self.logger.debug(f'Triggering sync for {group}')
                        if group in self.sync_trigger.values():
                            # group not yet finished
                            group = None
                if group:
                    with self.call_next_lock:
                        if group in self.call_next:
                            downstream = self.call_next.pop(group)
                            self.submit_chain(downstream)
            else:
                # normal result indicates chain continuation
                downstream = None
                with self.call_next_lock:
                    downstream = self.call_next.pop(result.task)
                if downstream:
                    self.logger.debug(f'Continuing chain {downstream}')
                    self.submit_chain(downstream)
