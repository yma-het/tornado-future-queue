from tornado import queues
from tornado.concurrent import Future
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado_retry_client import RetryClient, FailedRequest
from tornado import gen, ioloop
import sys


class HttpLongTimeout(Exception):
    """Base class for exceptions in this module."""
    def __init__(self):
        print "We have tried maaaany times. Giving up..."
        sys.exit(1)


q = queues.Queue(maxsize=10)

http_client = AsyncHTTPClient()
retry_client = RetryClient(http_client, max_retries=5)

@gen.coroutine
def consumer():
    while True:
        future = yield q.get()
        print "got from queue!"
        item = yield future
        print item
        q.task_done()

@gen.coroutine
def producer():
    for item in range(100):
        result = Future()
        yield q.put(result)
        io_loop.add_callback(worker, "http://mirror.internode.on.net/pub/test/1meg.test", result, item)
        print('Put %s' % item)

@gen.coroutine
def worker(url, future, iteration):
#def worker(url='http://ya.ru', future=Future()):
    #while True:
        #future = yield q_worker.get()
    print "Will fetch: %s from iteration: %s" % (url, iteration)
    httprequest = HTTPRequest(url,request_timeout=50,connect_timeout=50)
    try:
        #res = yield http_client.fetch(httprequest)
        response = yield retry_client.fetch(httprequest)
        future.set_result(response)
    except FailedRequest:
        raise HttpLongTimeout
        

@gen.coroutine
def main():
    consumer()           # Start consumer.
    yield producer()     # Wait for producer to put all tasks.
    yield worker()
    yield q.join()       # Wait for consumer to finish all tasks.
    print('Done')

io_loop = ioloop.IOLoop.current()
io_loop.run_sync(main)