# from uuid import uuid4
from itertools import count


# def id_counter():
#     for _ in count():
#         yield uuid4()


id_counter = count()


class Task:
    def __init__(self) -> None:
        self.id = next(id_counter)
    def __pow__(self, other):
        if isinstance(other, Task):
            # if `other` is last task in chain, make it singleton chain
            # so it is a proper chain
            other = Chain(other, End())
        return Chain(self, other)
    def __pos__(self):
        # mark task as end of chain
        return Chain(self, End())
    def __or__(self, other):
        this = Chain(self, End())
        if isinstance(other, Task):
            other = Chain(other, End())
        return Group({this, other})


class Single(Task):
    def __init__(self, func, *args) -> None:
        super().__init__()
        self.func = func
        self.args = args
    def __repr__(self):
        return f'{self.func.__name__}{self.args}'
    def run(self):
        return self.func(*self.args)
    __call__ = run


class Group(Task):
    def __init__(self, chains: set) -> None:
        super().__init__()
        self.chains = chains
    def __repr__(self):
        return f'g{self.id}({self.chains})'
    def __or__(self, another):
        if isinstance(another, AbstractChain):
            return Group(self.chains | {another})
        elif isinstance(another, Single):
            return Group(self.chains | {Chain(another, End())})
        elif isinstance(another, Group):
            # flatten groups
            return Group(self.chains | another.chains)


class AbstractChain:
    def __init__(self) -> None:
        self.id = next(id_counter)
    def __or__(self, other) -> Group:
        assert isinstance(other, AbstractChain)
        return Group({self, other})


class Chain(AbstractChain):
    def __init__(self, task: Task, chain: AbstractChain) -> None:
        super().__init__()
        self.task = task
        self.chain = chain
    def __repr__(self):
        return f'{self.task} ** {self.chain}'


class End(Task, AbstractChain):
    def __repr__(self):
        return f'End{self.id}'
    def run(self):
        return None


s = Single


def make_single(func):
    def _mk_sg(*args):
        return Single(func, *args)
    return _mk_sg


task = make_single
