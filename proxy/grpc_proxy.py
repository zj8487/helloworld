__all__ = ['GrpcProxy']

import traceback
import threading

import Queue

import go.logging

from grpc.beta import implementations
from grpc.framework.foundation import logging_pool
from grpc import StatusCode

from gevent.event import AsyncResult

import go.timer


class GeventTimer(object):
    '''
    Timer in gevent loop to iterating responses performed by grpc.
    '''

    def __init__(self, resp_queue, pending_response):
        self._resp_queue = resp_queue
        self._pending_response = pending_response
        self._timer = go.timer.new_timer(0.001, self.timeout_cb)

    def timeout_cb(self):
        self._timer = None

        try:
            while not self._resp_queue.empty():
                (seq, response, exception) = self._resp_queue.get()
                asyc_result = self._pending_response[seq]
                del self._pending_response[seq]
                if response:
                    asyc_result.set(response)
                else:
                    asyc_result.set_exception(exception)
        except Exception:
            traceback.print_exc()
        finally:
            self._timer = go.timer.new_timer(0.001, self.timeout_cb)


@go.logging.class_wrapper
class GrpcProxy(threading.Thread):
    '''
    Threaded proxy to grpc server which perform request/response from gevent loop.
    '''

    TIMEOUT_SECONDS = 1

    def __init__(self, host, port, stub_creator, pool_size=1):
        super(GrpcProxy, self).__init__()
        self.setDaemon(True)

        self._host = host
        self._port = port
        self._pool_size = pool_size
        self._pool = logging_pool.pool(self._pool_size)
        self._channel = implementations.insecure_channel(self._host, self._port)
        self._channel.subscribe(self.connectivity_update, try_to_connect=True)
        self._stub = stub_creator(self._channel, pool=self._pool)
        self._req_queue = Queue.Queue()
        self._resp_queue = Queue.Queue()

        self._seq = 0
        self._pending_responses = {}
        self._timer = GeventTimer(self._resp_queue, self._pending_responses)

    def connectivity_update(self, connectivity):
        pass
        # print threading.current_thread().name, connectivity

    def perform_request(self, method, request):
        self._seq += 1
        asyc_result = AsyncResult()
        self._pending_responses[self._seq] = asyc_result

        self._req_queue.put((self._seq, method, request))
        return asyc_result

    def run(self):
        while True:
            request_tuple = self._req_queue.get()
            if request_tuple is None:
                break
            (seq, method, request) = request_tuple
            stub_method = getattr(self._stub, method)
            response = stub_method.future(request, self.TIMEOUT_SECONDS)
            response.seq = seq
            response.add_done_callback(self.grpc_done_cb)
            # print "request queue:", self._req_queue.qsize(), "response queue:", self._resp_queue.qsize()

    def stop(self):
        self._req_queue.put(None)
        self._channel.unsubscribe(self.connectivity_update)
        del self._stub
        self._stub = None
        del self._channel
        self._channel = None

    def grpc_done_cb(self, future_response):
        code = future_response.code()
        if code == StatusCode.OK:
            exception = None
        else:
            exception = future_response.exception()

        seq = future_response.seq
        if exception:
            self._resp_queue.put((seq, None, exception))
        else:
            response = future_response.result()
            self._resp_queue.put((seq, response, None))
