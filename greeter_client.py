# Copyright 2015, Google Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""The Python implementation of the GRPC helloworld.Greeter client."""

import gevent.monkey
gevent.monkey.patch_socket()

from gevent.pool import Pool
pool = Pool(10)
spawn_func = pool.spawn

from proxy import GrpcProxy
import helloworld_pb2

_TIMEOUT_SECONDS = 10


class TestHello(object):

    def __init__(self, ):
        self._proxy1 = None
        self._proxy2 = None

    def start(self):
        self._spawn = spawn_func

        self._proxy1 = GrpcProxy('localhost', 50051, helloworld_pb2.beta_create_Greeter_stub)
        self._proxy1.start()
        self._request_let1 = self._spawn(self.run)
        self._request_let1.link(self.stop)

        self._proxy2 = GrpcProxy('localhost', 50051, helloworld_pb2.beta_create_Greeter_stub)
        self._proxy2.start()
        self._request_let2 = self._spawn(self.run2)
        self._request_let2.link(self.stop)

    def stop(self, t=None):
        self._request_let1.unlink(self.stop)
        self._request_let2.unlink(self.stop)

        self._proxy1.stop()
        del self._proxy1
        self._proxy1 = None

        self._proxy2.stop()
        del self._proxy2
        self._proxy2 = None

    def run(self):
        while True:
            gevent.sleep(0.001)

            request = helloworld_pb2.HelloRequest(name='you1')
            aync_result = self._proxy1.perform_request('SayHello', request)
            try:
                response = aync_result.get()
            except Exception, e:
                print "Greeter client error", e

            if response is not None:
                print("Greeter client received: " + response.message)
            else:
                print("Greeter client received None ")

    def run2(self):
        while True:
            gevent.sleep(0.02)

            request = helloworld_pb2.HelloRequest(name='you2')
            self._proxy2.perform_request('SayHello', request)


def main():
    test_obj = TestHello()
    test_obj.start()

    try:
        while True:
            gevent.sleep(1)
    except KeyboardInterrupt:
        test_obj.stop()


if __name__ == '__main__':
    main()
