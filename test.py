import gevent
from gevent import getcurrent
from gevent.pool import Group

group = Group()

def hello_from(n):
    if isinstance(n, int) is not True:
        raise TypeError
    print('{0},{1}'.format(str(n), getcurrent()))
    return '{0},{1}'.format(str(n), getcurrent())

a = [1, 2, 3, '4']
group.map(hello_from, a)

