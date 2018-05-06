from miniflow import Service
from miniflow import task
from time import sleep
import logging
import io
import unittest
from queue import Queue


class TestSugarFlow(unittest.TestCase):

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.service = Service(n_workers=4)

    def setUp(self):
        self.service.start()

    def test_flow(self):

        TIME_FACTOR = 0.05

        # use a thread-safe queue to store output of tasks
        q = Queue()

        @task
        def eat(name):
            q.put(f'++ Eating {name}')
            sleep(len(name) * TIME_FACTOR)
            q.put(f'-- eaten {name}')

        @task
        def drink(name):
            q.put(f'++ Drinking {name}')
            sleep(len(name) * TIME_FACTOR)
            q.put(f'-- drunk {name}')

        sugar_flow = (
            eat('dumping') ** (drink('soup') | drink('beer')) ** eat('roll')
            |
            eat('sausage') ** (eat('ice-cream') | eat('baked-cookie'))
            |
            eat('bread')
        ) ** drink('finish_spirit')

        self.service.submit_chain(sugar_flow)

        # wait for 1 second - workflow should already finish
        sleep(1)

        out_lines = []
        while q.qsize():
            out_lines.append(q.get())

        self.assertTrue('++ Eating dumping' in out_lines[0:3])
        self.assertTrue('++ Eating sausage' in out_lines[0:3])
        self.assertTrue('++ Eating bread' in out_lines[0:3])

        def before(line1, line2):
            i1 = out_lines.index(line1)
            i2 = out_lines.index(line2)
            return i1 < i2

        self.assertTrue(before('-- eaten dumping', '++ Drinking beer'))
        self.assertTrue(before('-- eaten dumping', '++ Drinking soup'))
        self.assertTrue(before('-- eaten sausage', '++ Eating ice-cream'))
        self.assertTrue(before('-- eaten sausage', '++ Eating baked-cookie'))

        self.assertTrue(before('++ Eating ice-cream', '++ Eating roll'))
        self.assertTrue(before('++ Eating baked-cookie', '++ Eating roll'))

        # 'roll' ends before 'baked-cookie' since the latter is fairly long
        self.assertTrue(before('-- eaten roll', '-- eaten baked-cookie'))

        self.assertTrue(before('-- eaten bread', '++ Drinking finish_spirit'))
        self.assertTrue(before('-- eaten roll', '++ Drinking finish_spirit'))
        self.assertTrue(before('-- eaten baked-cookie', '++ Drinking finish_spirit'))

    def tearDown(self):
        self.service.stop()


if __name__ == '__main__':
    unittest.main()
