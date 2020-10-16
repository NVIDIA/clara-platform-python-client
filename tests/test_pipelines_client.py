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

import nvidia_clara.grpc.common_pb2 as common_pb2
import nvidia_clara.grpc.pipelines_pb2 as pipelines_pb2

from nvidia_clara.base_client import BaseClient
from nvidia_clara.pipelines_client import PipelinesClient
import nvidia_clara.pipeline_types as pipeline_types

from tests.test_client_tools import run_client_test


def run_pipeline_client(stub, method_name, *args, **kwargs):
    with PipelinesClient(target='10.0.0.1:50051', stub=stub) as client:
        response = getattr(client, method_name)(*args, **kwargs)
        return response


class MockClaraPipelineServiceClient:
    stub_method_handlers = []

    def __init__(self, channel, stub=None, request_header=None, logger=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def create_pipeline(self, *args, **kwargs):
        return run_client_test(
            'Pipelines',
            'create_pipeline',
            run_pipeline_client,
            stub_method_handlers=MockClaraPipelineServiceClient.stub_method_handlers,
            *args, **kwargs)

    def list_pipelines(self, *args, **kwargs):
        return run_client_test(
            'Pipelines',
            'list_pipelines',
            run_pipeline_client,
            stub_method_handlers=MockClaraPipelineServiceClient.stub_method_handlers,
            *args, **kwargs)

    def close(self):
        pass


PIPELINE_TEXT = '''api-version: 0.2.0
name: sample-pipeline
operators:
  - name: producer
    import:
      path: producer.yaml
  - name: consumer
    import:
      path: consumer.yaml
      args:
        input-from: producer
'''


def test_create_pipeline():
    pipeline_yaml = 'pipeline.yaml'

    requests = [
        pipelines_pb2.PipelinesCreateRequest(
            header=BaseClient.get_request_header(),
            definition=pipelines_pb2.PipelineDefinitionFile(
                path='pipeline.yaml',
                content=PIPELINE_TEXT)
        )
    ]

    responses = [
        pipelines_pb2.PipelinesCreateResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            pipeline_id=common_pb2.Identifier(
                value='92656d79fa414db6b294069c0e9e6df5'
            )
        )
    ]

    stub_method_handlers = [(
        'Create',
        'stream_unary',
        (
            requests,
            responses
        )
    )]

    # set handlers
    MockClaraPipelineServiceClient.stub_method_handlers = stub_method_handlers

    def_list = [
        pipeline_types.PipelineDefinition(name=pipeline_yaml, content=PIPELINE_TEXT)
    ]

    with MockClaraPipelineServiceClient('localhost:50051') as client:
        pipeline_id = client.create_pipeline(definition=def_list)
        print(pipeline_id)
        assert pipeline_id.value == '92656d79fa414db6b294069c0e9e6df5'


def test_create_pipeline_with_id():
    pipeline_yaml = 'pipeline.yaml'

    requests = [
        pipelines_pb2.PipelinesCreateRequest(
            header=BaseClient.get_request_header(),
            pipeline_id=common_pb2.Identifier(
                value='92656d79fa414db6b294069c0e9e6df5'
            ),
            definition=pipelines_pb2.PipelineDefinitionFile(
                path='pipeline.yaml',
                content=PIPELINE_TEXT)
        )
    ]

    responses = [
        pipelines_pb2.PipelinesCreateResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            pipeline_id=common_pb2.Identifier(
                value='92656d79fa414db6b294069c0e9e6df5'
            )
        )
    ]

    stub_method_handlers = [(
        'Create',
        'stream_unary',
        (
            requests,
            responses
        )
    )]

    # set handlers
    MockClaraPipelineServiceClient.stub_method_handlers = stub_method_handlers

    def_list = [
        pipeline_types.PipelineDefinition(name=pipeline_yaml, content=PIPELINE_TEXT)
    ]

    pipeline_id = pipeline_types.PipelineId('92656d79fa414db6b294069c0e9e6df5')

    with MockClaraPipelineServiceClient('localhost:50051') as client:
        pipeline_id = client.create_pipeline(definition=def_list, pipeline_id=pipeline_id)
        print(pipeline_id)
        assert pipeline_id.value == '92656d79fa414db6b294069c0e9e6df5'


def test_list_pipeline():
    requests = [
        pipelines_pb2.PipelinesListRequest(
            header=BaseClient.get_request_header()
        )
    ]

    responses = [
        pipelines_pb2.PipelinesListResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            details=pipelines_pb2.PipelinesListResponse.PipelineDetails(
                name='Pipeline_1',
                pipeline_id=common_pb2.Identifier(
                    value='92656d79fa414db6b294069c0e9e6df5'
                )
            )
        ),
        pipelines_pb2.PipelinesListResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            details=pipelines_pb2.PipelinesListResponse.PipelineDetails(
                name='Pipeline_2',
                pipeline_id=common_pb2.Identifier(
                    value='21656d79fa414db6b294069c0e9e6r23'
                )
            )
        )
    ]

    stub_method_handlers = [(
        'List',
        'unary_stream',
        (
            requests,
            responses
        )
    )]

    # set handlers
    MockClaraPipelineServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraPipelineServiceClient('localhost:50051') as client:
        pipeline_list = client.list_pipelines()

        print(pipeline_list)

        assert len(pipeline_list) == 2
        assert pipeline_list[0].pipeline_id.value == '92656d79fa414db6b294069c0e9e6df5'
        assert pipeline_list[1].pipeline_id.value == '21656d79fa414db6b294069c0e9e6r23'
