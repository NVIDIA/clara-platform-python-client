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

from enum import Enum
from typing import List, Mapping
from nvidia_clara.grpc import common_pb2, payloads_pb2


class PayloadType(Enum):
    Unknown = payloads_pb2.PAYLOAD_TYPE_UNKNOWN

    Pipeline = payloads_pb2.PAYLOAD_TYPE_PIPELINE

    Reusable = payloads_pb2.PAYLOAD_TYPE_REUSABLE

    Minimum = Pipeline

    Maximum = Reusable


class PayloadFileDetails:

    def __init__(self, other: payloads_pb2.PayloadFileDetails = None, mode: int = None, name: str = None,
                 size: int = None):
        """
        Args:
            mode(int): Permissions
            name(str): File Path Location
            size(int): Size of File
            other(payloads_pb2.PayloadFileDetails): If specified, object information replicated
        """
        if other is None:
            if mode is None:
                raise Exception("Mode parameter must be initalized to a non-null integer value")
            if (name is None) or (name == ""):
                raise Exception("Name parameter must be initalized to a non-null string value")
            if size is None:
                raise Exception("Size must be  to a non-null integer value")
            self._mode = mode
            self._name = name
            self._size = size
        else:
            self._mode = other.mode
            self._name = other.name
            self._size = other.size

    @property
    def mode(self):
        """
        Mode of the file.

        See [https://en.wikipedia.org/wiki/Chmod] for additional information.
        """
        return self._mode

    @mode.setter
    def mode(self, mode: int):
        """
        Mode of the file.

        See [https://en.wikipedia.org/wiki/Chmod] for additional information.
        """
        self._mode = mode

    @property
    def name(self):
        """
        Unique (withing a payload) name of the file; in path format.

        File names are relative to the root of the payload, and should not be rooted paths (prefixed with a '/' character).
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Unique (withing a payload) name of the file; in path format.

        File names are relative to the root of the payload, and should not be rooted paths (prefixed with a '/' character).
        """
        self._name = name

    @property
    def size(self):
        """Size, in bytes, of the file."""
        return self._size

    @size.setter
    def size(self, size: int):
        """Size, in bytes, of the file."""
        self._size = size

    def _eq_(self, other):
        return (self._mode == other.getMode()) and (self._name == other.getName()) and (
                self._size == other.getSize())

    def _ne_(self, other):
        return not (self == other)

    def _hash_(self):
        return hash((self._mode, self._name, self._size))


class PayloadId:

    def __init__(self, value: str = None):
        if value == None:
            raise Exception("Arguement 'Value' must be initialized to non-null or empty string")

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


class PayloadDetails:

    def __init__(self, payload_id: PayloadId = None, file_details: List[PayloadFileDetails] = None,
                 payload_type: payloads_pb2.PayloadType = None, metadata: Mapping[str, str] = None):
        if file_details is None:
            file_details = []
        if metadata is None:
            metadata = dict()

        self._payload_id = payload_id
        self._file_details = file_details
        self._payload_type = payload_type
        self._metadata = metadata

    @property
    def payload_id(self):
        """Gets the unique identifier of the payload."""
        return self._payload_id

    @payload_id.setter
    def payload_id(self, payload_id: PayloadId):
        """Sets the unique identifier of the payload."""
        self._payload_id = payload_id

    @property
    def file_details(self):
        """Gets list of files contained in the payload."""
        return self._file_details

    @file_details.setter
    def file_details(self, file_details: List[PayloadFileDetails]):
        """Sets a list of files contained in the payload."""
        self._file_details = file_details

    @property
    def payload_type(self):
        """Gets a list of files contained in the payload."""
        return self._payload_type

    @payload_type.setter
    def payload_type(self, payload_type: payloads_pb2.PayloadType):
        """Sets a list of files contained in the payload."""
        self._payload_type = payload_type

    @property
    def metadata(self) -> Mapping[str, str]:
        """
        Metadata (set of key/value pairs) associated with the payload
        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Mapping[str, str]):
        """
        Metadata (set of key/value pairs) associated with the payload
        """
        self._metadata = metadata
