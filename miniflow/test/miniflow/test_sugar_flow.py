from miniflow import Service
from miniflow import task
from time import sleep


@task
def eat(name):
    print('++ Eating', name)
    sleep(max(1, len(name) - 3))
    print('-- eaten', name)


@task
def drink(name):
    print('++ Drinking', name)
    sleep(max(1, len(name) - 3))
    print('-- drunk', name)


sugar_flow = (
    eat('dumping') ** (drink('soup') | drink('beer')) ** eat('roll')
    |
    eat('sausage') ** (eat('ice-cream') | eat('baked-cookie'))
    |
    eat('bread')
) ** drink('finish_spirit')


e = Service(n_workers=4)
e.logger.setLevel('INFO')
e.start()
e.submit_chain(sugar_flow)
