"""Example on how to use dask.delayed.

http://matthewrocklin.com/blog/work/2017/01/24/dask-custom
"""
import random
from time import sleep
from dask import delayed

# define some fake function to simulate the actual work


def load(address):
    sleep(random.random() / 2)


def load_from_sql(address):
    sleep(random.random() / 2 + 0.5)


def process(data, reference):
    sleep(random.random() / 2)


def roll(a, b, c):
    sleep(random.random() / 5)


def compare(a, b):
    sleep(random.random() / 10)


@delayed
def reduction(seq):
    sleep(random.random() / 1)

# delayed changes a function so that instead of running immediately it captures
# its inputs and puts everything into a task graph for future execution.
load = delayed(load)
load_from_sql = delayed(load_from_sql)
process = delayed(process)
roll = delayed(roll)
compare = delayed(compare)
# reduction = delayed(reduction)


def main():
    filenames = ['mydata-%d.dat' % i for i in range(10)]
    data = [load(fn) for fn in filenames]

    reference = load_from_sql('sql://mytable')
    processed = [process(d, reference) for d in data]

    rolled = []
    for i in range(len(processed) - 2):
        a = processed[i]
        b = processed[i + 1]
        c = processed[i + 2]
        r = roll(a, b, c)
        rolled.append(r)

    compared = []
    for i in range(200):
        a = random.choice(rolled)
        b = random.choice(rolled)
        c = compare(a, b)
        compared.append(c)

    best = reduction(compared)

    # visualize the task graph with Graphviz.
    best.visualize('delayed_example.svg')
    best.visualize('delayed_example.pdf')

    # run the actual computation
    result = best.compute()


if __name__ == '__main__':
    main()
