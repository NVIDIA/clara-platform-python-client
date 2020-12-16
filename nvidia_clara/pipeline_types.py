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
from nvidia_clara.grpc import common_pb2


class PipelineDefinition:

    def __init__(self, content: str = None, name: str = None):
        self._content = content
        self._name = name

    @property
    def content(self) -> str:
        """Text content of a pipeline definition.

        Content is typed as a "System.String" to avoid encoding related issues."""
        return self._content

    @content.setter
    def content(self, content: str):
        """Text content of a pipeline definition.

        Content is typed as a "System.String" to avoid encoding related issues."""
        self._content = content

    @property
    def name(self) -> str:
        """The name of the pipeline definition.

        Not the name of the pipeline as defined by the definition.

        Example: clara/examples/my-pipeline.yml
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """The name of the pipeline definition.

        Not the name of the pipeline as defined by the definition.

        Example: clara/examples/my-pipeline.yml
        """
        self._name = name


class PipelineId:

    def __init__(self, value: str):
        if (value == "") or (value is None):
            raise Exception("Value arguement must be initialized to non-null value")

        self._value = value

    @property
    def value(self):
        return self._value

    def _eq_(self, other):
        return self._value == other._value

    def _ne_(self, other):
        return not (self == other)

    def _repr_(self):
        return "%s" % (self._value)

    def _str_(self):
        return "%s" % (self._value)

    def _hash_(self):
        return hash(self._value)

    def to_grpc_value(self):
        id = common_pb2.Identifier()
        id.value = self._value
        return id


class PipelineDetails:

    def __init__(self, pipeline_id: PipelineId = None, name: str = None, definition: List[PipelineDefinition] = None,
                 metadata: Mapping[str, str] = None):
        if definition is None:
            definition = []
        if metadata is None:
            metadata = dict()
        self._pipeline_id = pipeline_id
        self._name = name
        self._definition = definition
        self._metadata = metadata

    @property
    def pipeline_id(self) -> PipelineId:
        """Unique identifier of the pipeline."""
        return self._pipeline_id

    @pipeline_id.setter
    def pipeline_id(self, pipeline_id: PipelineId):
        """Unique identifier of the pipeline."""
        self._pipeline_id = pipeline_id

    @property
    def name(self) -> str:
        """
        Human readable name of the pipeline.

        Not guaranteed to be unique.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Human readable name of the pipeline.

        Not guaranteed to be unique.
        """
        self._name = name

    @property
    def definition(self) -> List[PipelineDefinition]:
        """
        The definition of the pipeline.

        Clara pipeline definitions can be multi-file.
        """
        return self._definition

    @definition.setter
    def definition(self, definition: List[PipelineDefinition]):
        """
        The definition of the pipeline.

        Clara pipeline definitions can be multi-file.
        """
        self._definition = definition

    @property
    def metadata(self) -> Mapping[str, str]:
        """
        Metadata (set of key/value pairs) associated with the pipeline
        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Mapping[str, str]):
        """
        Metadata (set of key/value pairs) associated with the pipeline
        """
        self._metadata = metadata


class PipelineInfo:

    def __init__(self, pipeline_id: PipelineId = None, name: str = None, metadata: Mapping[str, str] = None):
        if metadata is None:
            metadata = dict()
        self._pipeline_id = pipeline_id
        self._name = name
        self._metadata = metadata

    @property
    def pipeline_id(self) -> PipelineId:
        """Unique identifier of the pipeline."""
        return self._pipeline_id

    @pipeline_id.setter
    def pipeline_id(self, pipeline_id: PipelineId):
        """Unique identifier of the pipeline."""
        self._pipeline_id = pipeline_id

    @property
    def name(self) -> str:
        """
        Human readable name of the pipeline.

        Not guaranteed to be unique.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Human readable name of the pipeline.

        Not guaranteed to be unique.
        """
        self._name = name

    @property
    def metadata(self) -> Mapping[str, str]:
        """
        Metadata (set of key/value pairs) associated with the pipeline
        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Mapping[str, str]):
        """
        Metadata (set of key/value pairs) associated with the pipeline
        """
        self._metadata = metadata
