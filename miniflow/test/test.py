from executor import Service
from canvas import task
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


# flow = \
#     Chain(
#         Group({
#             Chain(
#                 Single(eat, 'dumpling'),
#                 Chain(
#                     Group({
#                         Chain(Single(drink, 'soup'), End()),
#                         Chain(Single(drink, 'sprite'), End())
#                     }),
#                     Chain(
#                         Single(eat, 'roll'),
#                         End()))),
#             Chain(
#                 Single(eat, 'sausage'),
#                 Chain(
#                     Group({
#                         Chain(Single(eat, 'ice-cream'), End()),
#                         Chain(Single(eat, 'cake'), End()),
#                     }), End()))
#         }),
#         End())

sugar_flow = (
    eat('dumping') ** (drink('soup') | drink('beer')) ** eat('roll')
    |
    eat('sausage') ** (eat('ice-cream') | eat('baked-cookie'))
    |
    eat('bread')
) ** drink('finish_spirit')

# e = Service(n_workers=2)
# e = Service(n_workers=3)
e = Service(n_workers=4)
e.logger.setLevel('INFO')
e.start()
e.submit_chain(sugar_flow)
# e.submit_chain(flow)
