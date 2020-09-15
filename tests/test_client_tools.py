# Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import signal
import time

import grpc
import grpc_testing
from grpc.framework.foundation import logging_pool

import nvidia_clara.grpc.common_pb2 as common_pb2
import nvidia_clara.grpc.jobs_pb2 as jobs_pb2
import nvidia_clara.grpc.jobs_pb2_grpc as jobs_pb2_grpc
import nvidia_clara.grpc.payloads_pb2 as payloads_pb2
import nvidia_clara.grpc.payloads_pb2_grpc as payloads_pb2_grpc
import nvidia_clara.grpc.pipelines_pb2 as pipelines_pb2
import nvidia_clara.grpc.pipelines_pb2_grpc as pipelines_pb2_grpc

SERVICES = {
    'Pipelines': pipelines_pb2.DESCRIPTOR.services_by_name,
    'Jobs': jobs_pb2.DESCRIPTOR.services_by_name,
    'Payloads': payloads_pb2.DESCRIPTOR.services_by_name
}


def get_stubs(service, channel):
    if service == 'Jobs':
        return jobs_pb2_grpc.JobsStub(channel)
    elif service == 'Payloads':
        return payloads_pb2_grpc.PayloadsStub(channel)
    elif service == 'Pipelines':
        return pipelines_pb2_grpc.PipelinesStub(channel)


class Timeout(Exception):
    pass


# Reference: https://github.com/grpc/grpc/blob/master/src/python/grpcio_tests/tests/testing/_client_test.py
def verify_request(channel, stub_method, call_sig, expected_requests, responses, timeout=1):
    def timeout_handler(signum, frame):
        raise Timeout('Timeout while taking requests')

    try:
        # setting up timeout handler because grpc_testing module doesn't support timeout for take_xxx_xxx methods
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        if call_sig == 'stream_unary':
            invocation_metadata, rpc = channel.take_stream_unary(stub_method)
            rpc.send_initial_metadata(())
            for expected_request in expected_requests:
                request = rpc.take_request()
                assert expected_request == request
            rpc.requests_closed()
            rpc.terminate(next(iter(responses)), (), grpc.StatusCode.OK, '')
        elif call_sig == 'unary_stream':
            invocation_metadata, request, rpc = channel.take_unary_stream(stub_method)
            assert next(iter(expected_requests)) == request
            rpc.send_initial_metadata(())
            for response in responses:
                rpc.send_response(response)
            rpc.terminate((), grpc.StatusCode.OK, '')
        elif call_sig == 'unary_unary':
            invocation_metadata, request, rpc = channel.take_unary_unary(stub_method)
            assert next(iter(expected_requests)) == request
            rpc.send_initial_metadata(())
            rpc.terminate(next(iter(responses)), (), grpc.StatusCode.OK, '')
    except Timeout:
        raise
    finally:
        signal.alarm(0)


def run_client_test(service_name, method_name, test_method, stub_method_handlers, *args, **kwargs):
    fake_time = grpc_testing.strict_fake_time(
        time.time())
    channel = grpc_testing.channel(SERVICES[service_name].values(),
                                   fake_time)
    stub = get_stubs(service_name, channel)
    service = SERVICES[service_name][service_name]

    client_execution_thread_pool = logging_pool.pool(1)
    try:
        test_client_only = kwargs.pop('_test_client_only', None)
        application_future = client_execution_thread_pool.submit(
            test_method,
            stub, method_name, *args, **kwargs)

        # if the client method call is expected to raise exception before grpc call
        if test_client_only:
            pass  # do not simulate grpc response
        else:
            for stub_method_name, call_sig, handlers in stub_method_handlers:
                expected_requests, responses = handlers
                stub_method = service.methods_by_name[stub_method_name]
                verify_request(channel, stub_method, call_sig, expected_requests, responses)

        application_return_value = application_future.result()
        application_exception = application_future.exception()
        if application_exception:
            raise application_exception
        return application_return_value
    except Timeout:
        raise
    finally:
        client_execution_thread_pool.shutdown(False)
        del channel
