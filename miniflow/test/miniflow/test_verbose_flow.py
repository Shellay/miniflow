from miniflow import Service
from miniflow import task
from miniflow.canvas import Single, Chain, Group, End
from time import sleep


def eat(name):
    print('++ Eating', name)
    sleep(max(1, len(name) - 3))
    print('-- eaten', name)


def drink(name):
    print('++ Drinking', name)
    sleep(max(1, len(name) - 3))
    print('-- drunk', name)


flow = Chain(
    Group({
        Chain(
            Single(eat, 'dumpling'),
            Chain(
                Group({
                    Chain(Single(drink, 'soup'), End()),
                    Chain(Single(drink, 'beer'), End())
                }),
                Chain(
                    Single(eat, 'roll'), End()))),
        Chain(
            Single(eat, 'sausage'),
            Chain(
                Group({
                    Chain(Single(eat, 'ice-cream'), End()),
                    Chain(Single(eat, 'baked-cookie'), End()),
                }), End())),
        Chain(
            Single(eat, 'bread'),
            End()),
    }),
    Chain(
        Single(drink, 'finish_spirit'),
        End()))


e = Service(n_workers=4)
e.logger.setLevel('DEBUG')
e.start()
e.submit_chain(flow)
