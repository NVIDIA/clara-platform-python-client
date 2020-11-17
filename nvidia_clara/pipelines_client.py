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

from typing import List, Mapping
import grpc
from nvidia_clara.grpc import pipelines_pb2, pipelines_pb2_grpc
import nvidia_clara.pipeline_types as pipeline_types
from nvidia_clara.base_client import BaseClient, RequestIterator


class PipelinesClientStub:
    def create_pipeline(self, definition: List[pipeline_types.PipelineDefinition],
                        pipeline_id: pipeline_types.PipelineId = None,
                        metadata: Mapping[str, str] = None) -> pipeline_types.PipelineId:
        """
        Requests the creation of a new pipeline by Clara.

        Args:
            definition(List[pipeline_types.PipelineDefinition]): Definition from which to create the new pipeline.
            pipeline_id:  Optional argument to force a specific pipeline identifier when replicating deployments.
                    Use ONLY with a high available primary-primary fail-over solution in place AND full understanding on
                    what it does.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the pipeline metadata. If a metadata
                    key in the request already exists in the pipeline record, or if duplicate keys are passed in the
                    request, the pipeline will not be updated and and an error will be returned. Keys are compared using
                    case insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes,
                    while the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the
                    overall metadata of an individual job is 4 Megabytes.

        Returns:
            pipeline_types.PipelineId of newly created pipeline
        """
        pass

    def list_pipelines(self) -> List[pipeline_types.PipelineInfo]:
        """
        Requests a list of pipelines from Clara.

        Returns:
            List of pipeline_types.PipelineInfo with running pipeline information
        """
        pass

    def pipeline_details(self, pipeline_id: pipeline_types.PipelineId) -> pipeline_types.PipelineDetails:
        """
        Requests details of a pipeline, identified by pipeline_types.PipelineId, from Clara.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the pipeline.

        Return:
            A pipeline_types.PipelineDetails instance with details on the pipeline specified by 'pipeline_id'
        """
        pass

    def remove_pipeline(self, pipeline_id: pipeline_types.PipelineId):
        """
        Removes a pipeline, identified by "pipelineId", from Clara.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the
                pipeline
        """
        pass

    def update_pipeline(self, pipeline_id: pipeline_types.PipelineId,
                        definition: List[pipeline_types.PipelineDefinition]):
        """
        Requests a pipeline, identified by "pipelineId", be updated by Clara.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the
                pipeline.
            definition: Definition from which to update the pipeline.
        """
        pass

    def add_metadata(self, pipeline_id: pipeline_types.PipelineId, metadata: Mapping[str, str]) -> Mapping[str, str]:
        """
        Requests the addition of metadata to a pipeline.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the pipeline whose metadata is to be appended.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the pipeline metadata. If a metadata
                    key in the request already exists in the pipeline record, or if duplicate keys are passed in the
                    request, the pipeline will not be updated and and an error will be returned. Keys are compared using
                    case insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes,
                    while the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the
                    overall metadata of an individual job is 4 Megabytes.

        Returns:
            A Mapping[str, str] containing the appended metadata
        """
        pass

    def remove_metadata(self, pipeline_id: pipeline_types.PipelineId, keys: List[str]) -> Mapping[str, str]:
        """
        Requests the removal of specified metadata of a pipeline.

        Args:
            pipeline_id: Unique identifier of the pipeline whose metadata is to be removed.
            keys: List of keys to be removed from the pipeline metadata.

        Returns:
            A Mapping[str, str] containing the updated set of metadata
        """
        pass


class PipelinesClient(BaseClient, PipelinesClientStub):
    def __init__(self, target: str, port: str = None, stub=None):
        """
        Pipelines Client Creation

        Args:
            target (str): ipv4 address of clara instance
            port (str): if specified, port will be appended to the target with a ":"
        """
        if target is None:
            raise Exception("Target must be initialized to a non-null value")

        self._connection = target

        if port is not None:
            self._connection += ":" + port

        self._channel = grpc.insecure_channel(self._connection)

        if stub is None:
            self._stub = pipelines_pb2_grpc.PipelinesStub(self._channel)
        else:
            self._stub = stub

    def close(self):
        """
        Close connection
        """
        if self._channel:
            self._channel.close()
            self._channel = None
        else:
            print("Connection for client already closed")

    def reconnect(self):
        """
        Re-open connection with existing channel
        """
        if self._channel is None:
            self._channel = grpc.insecure_channel(self._connection)
            self._stub = pipelines_pb2_grpc.PipelinesStub(self._channel)
        else:
            print("Connection for client already open")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._channel is not None:
            self.close()
        return False

    def create_pipeline(self, definition: List[pipeline_types.PipelineDefinition],
                        pipeline_id: pipeline_types.PipelineId = None, metadata: Mapping[str, str] = None,
                        timeout=None) -> pipeline_types.PipelineId:
        """
        Requests the creation of a new pipeline by Clara.

        Args:
            definition(List[pipeline_types.PipelineDefinition]): Definition from which to create the new pipeline.
            pipeline_id:  Optional argument to force a specific pipeline identifier when replicating deployments.
                    Use ONLY with a high available primary-primary fail-over solution in place AND full understanding on
                    what it does.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the pipeline metadata. If a metadata
                    key in the request already exists in the pipeline record, or if duplicate keys are passed in the
                    request, the pipeline will not be updated and and an error will be returned. Keys are compared using
                    case insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes,
                    while the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the
                    overall metadata of an individual job is 4 Megabytes.

        Returns:
            pipeline_types.PipelineId of newly created pipeline
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if definition is None:
            raise Exception("Argument 'definition' must be initialized to a non-null list instance")

        request_list = []

        for item in definition:
            item_definition = pipelines_pb2.PipelineDefinitionFile(
                content=item.content,
                path=item.name
            )

            # If pipeline identifier set, must first be in GRPC Identifier format
            if pipeline_id is not None:
                pipeline_id = pipeline_id.to_grpc_value()

            request = pipelines_pb2.PipelinesCreateRequest(
                definition=item_definition,
                pipeline_id=pipeline_id,
                header=self.get_request_header()
            )

            if metadata is not None:
                request.metadata.update(metadata)

            request_list.append(request)

        request_list = RequestIterator(request_list)

        response = self._stub.Create(
            request_list(),
            timeout=timeout
        )

        self.check_response_header(header=response.header)

        return pipeline_types.PipelineId(response.pipeline_id.value)

    def list_pipelines(self, timeout=None) -> List[pipeline_types.PipelineInfo]:
        """
        Requests a list of pipelines from Clara.

        Returns:
            List of pipeline_types.PipelineInfo with running pipeline information
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = pipelines_pb2.PipelinesListRequest(
            header=self.get_request_header()
        )

        response = self._stub.List(request, timeout=timeout)

        info_list = []

        responses = [resp for resp in response]

        if len(responses) > 0:
            for item in responses:
                if (item.details is None) or (item.details.pipeline_id.value == ''):
                    continue

                info = pipeline_types.PipelineInfo(
                    pipeline_id=pipeline_types.PipelineId(item.details.pipeline_id.value),
                    name=item.details.name,
                    metadata=item.details.metadata
                )

                info_list.append(info)

        return info_list

    def pipeline_details(self, pipeline_id: pipeline_types.PipelineId, timeout=None) -> pipeline_types.PipelineDetails:
        """
        Requests details of a pipeline, identified by pipeline_types.PipelineId, from Clara.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the pipeline.

        Return:
            A pipeline_types.PipelineDetails instance with details on the pipeline specified by 'pipeline_id'
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if pipeline_id.value is None or pipeline_id.value == "":
            raise Exception("Pipeline identifier argument must be initialized with non-null instance")

        request = pipelines_pb2.PipelinesDetailsRequest(
            header=self.get_request_header(),
            pipeline_id=pipeline_id.to_grpc_value(),
        )

        response = self._stub.Details(request, timeout=timeout)

        responses = [resp for resp in response]

        if len(responses) > 0:
            self.check_response_header(header=responses[0].header)

            result = pipeline_types.PipelineDetails(
                name=responses[0].name,
                pipeline_id=pipeline_types.PipelineId(responses[0].pipeline_id.value),
                metadata=responses[0].metadata
            )

            result_definition = []

            for resp in responses:
                result_definition.append(
                    pipeline_types.PipelineDefinition(
                        name=resp.name,
                        content=resp.definition
                    )
                )

            result.definition = result_definition

            return result

        return None

    def remove_pipeline(self, pipeline_id: pipeline_types.PipelineId, timeout=None):
        """
        Removes a pipeline, identified by "pipelineId", from Clara.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the
                pipeline
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if pipeline_id.value is None or pipeline_id.value == "":
            raise Exception("Pipeline identifier argument must be initialized with non-null instance")

        request = pipelines_pb2.PipelinesRemoveRequest(
            header=self.get_request_header(),
            pipeline_id=pipeline_id.to_grpc_value()
        )

        response = self._stub.Remove(request, timeout=timeout)

        self.check_response_header(header=response.header)

    def update_pipeline(self, pipeline_id: pipeline_types.PipelineId,
                        definition: List[pipeline_types.PipelineDefinition],
                        timeout=None):
        """
        Requests a pipeline, identified by "pipelineId", be updated by Clara.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the
                pipeline.
            definition: Definition from which to update the pipeline.
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if pipeline_id.value is None or pipeline_id.value == "":
            raise Exception("Pipeline identifier argument must be initialized with non-null instance")

        if definition is None:
            raise Exception(
                "Pipeline definition argument must be initialized"
                "with non-null instnace of List[pipeline_types.PipelineDefinition]")

        request_list = []

        for item in definition:
            request = pipelines_pb2.PipelinesUpdateRequest(
                definition=pipelines_pb2.PipelineDefinitionFile(
                    content=item.content,
                    path=item.name
                ),
                header=self.get_request_header(),
                pipeline_id=pipeline_id.to_grpc_value()
            )

            request_list.append(request)

        request_list = RequestIterator(request_list)

        response = self._stub.Update(request_list(), timeout=timeout)

        self.check_response_header(header=response.header)

    def add_metadata(self, pipeline_id: pipeline_types.PipelineId, metadata: Mapping[str, str], timeout=None) -> \
            Mapping[str, str]:
        """
        Requests the addition of metadata to a pipeline.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the pipeline whose metadata is to be appended.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the pipeline metadata. If a metadata
                    key in the request already exists in the pipeline record, or if duplicate keys are passed in the
                    request, the pipeline will not be updated and and an error will be returned. Keys are compared using
                    case insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes,
                    while the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the
                    overall metadata of an individual job is 4 Megabytes.

        Returns:
            A Mapping[str, str] containing the appended metadata
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (pipeline_id.value is None) or (pipeline_id.value == ""):
            raise Exception("Pipeline identifier must have instantiated value")

        if metadata is None:
            raise Exception("Metadata must be an instantiated map")

        request = pipelines_pb2.PipelinesAddMetadataRequest(
            pipeline_id=pipeline_id.to_grpc_value()
        )

        request.metadata.update(metadata)

        response = self._stub.AddMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result

    def remove_metadata(self, pipeline_id: pipeline_types.PipelineId, keys: List[str], timeout=None) -> Mapping[
        str, str]:
        """
        Requests the removal of specified metadata of a pipeline.

        Args:
            pipeline_id: Unique identifier of the pipeline whose metadata is to be removed.
            keys: List of keys to be removed from the pipeline metadata.

        Returns:
            A Mapping[str, str] containing the updated set of metadata
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (pipeline_id.value is None) or (pipeline_id.value == ""):
            raise Exception("Pipeline identifier must have instantiated value")

        if keys is None:
            raise Exception("Keys paramater must be valid list of metadata keys")

        request = pipelines_pb2.PipelinesRemoveMetadataRequest(
            pipeline_id=pipeline_id.to_grpc_value()
        )

        request.keys.extend(keys)

        response = self._stub.RemoveMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result
