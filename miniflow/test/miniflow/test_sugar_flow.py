from miniflow import Service
from miniflow import task
from time import sleep
import logging
import io
import unittest


class TestSugarFlow(unittest.TestCase):

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        buf = io.StringIO()
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(buf)
        ch.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(ch)

        self.output_buffer = buf
        self.logger = logger
        self.service = Service(n_workers=4)

    def setUp(self):
        self.service.start()

    def test_flow(self):

        TIME_FACTOR = 0.05

        @task
        def eat(name):
            self.logger.info(f'++ Eating {name}')
            sleep(len(name) * TIME_FACTOR)
            self.logger.info(f'-- eaten {name}')

        @task
        def drink(name):
            self.logger.info(f'++ Drinking {name}')
            sleep(len(name) * TIME_FACTOR)
            self.logger.info(f'-- drunk {name}')

        sugar_flow = (
            eat('dumping') ** (drink('soup') | drink('beer')) ** eat('roll')
            |
            eat('sausage') ** (eat('ice-cream') | eat('baked-cookie'))
            |
            eat('bread')
        ) ** drink('finish_spirit')

        self.service.submit_chain(sugar_flow)

        sleep(1)

        self.output_buffer.seek(0)
        out = self.output_buffer.read()
        # print(out)
        out_lines = out.split('\n')
        # print(out_lines[0:3])

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
