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

import grpc
from typing import BinaryIO, Mapping, List
from nvidia_clara.grpc import payloads_pb2, payloads_pb2_grpc
from nvidia_clara.base_client import BaseClient
import nvidia_clara.payload_types as payload_types


class PayloadsClientStub:

    def create_payload(self, metadata: Mapping[str, str] = None) -> payload_types.PayloadDetails:
        """
        Creates a static payload.

        Payloads created using this API are created with a type of "PayloadType.Reusable"

        Returns:
             the details of newly created payload.
        """
        pass

    def delete_payload(self, payload_id: payload_types.PayloadId):
        """
        Requests the deletion of a payload, identified by "payload_id" from Clara.

        Deleted payloads cannot be recovered.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload to delete.
        """
        pass

    def get_details(self, payload_id: payload_types.PayloadId) -> payload_types.PayloadDetails:
        """
        Requests the details of a payload, identified by "payload_id" from Clara.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
        Returns:
            A payload_types.PayloadDetails instance containing payload details
        """
        pass

    def download_from(self, payload_id: payload_types.PayloadId, blob_name: str,
                      dest_obj: BinaryIO) -> payload_types.PayloadFileDetails:
        """
        Downloads a blob, identified by "blob_name", from a payload, identified by its "payload_id", from Clara.

        Data downloaded from the payload will be written to 'dest_obj' a passed in BinaryIO object with write privileges

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            blob_name (str): The name, or path, of the blob in the payload.
            dest_obj (BinaryIO): Target stream object to write to
        """
        pass

    def remove_from(self, payload_id: payload_types.PayloadId, blob_name: str):
        """
        Removes a blob from the payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload from which to remove the blob.
            blob_name (str): The name, or path, of the blob in the payload.
        """
        pass

    def upload(self, payload_id: payload_types.PayloadId, blob_name: str,
               file_object: BinaryIO = None) -> payload_types.PayloadFileDetails:
        """
        Uploads a blob from "file_object", to a Clara Payload identified by "payload_id".

        Each uploaded blob must be have a unique "blob_name" value within a given payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            blob_name (str): The name, or path, of the blob in the payload.
            file_object (BinaryIO): stream to read from and upload
        """
        pass

    def add_metadata(self, payload_id: payload_types.PayloadId, metadata: Mapping[str, str]) -> Mapping[str, str]:
        """
        Requests the addition of metadata to a payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the payload metadata. If a metadata
                    key in the request already exists in the job record, or if duplicate keys are passed in the request,
                    the payload will not be updated and and an error will be returned. Keys are compared using case
                    insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes, while
                    the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the overall
                    metadata of an individual payload is 4 Megabytes.

        Returns:
            A Mapping[str, str] containing the appended metadata
        """
        pass

    def remove_metadata(self, payload_id: payload_types.PayloadId, keys: List[str]) -> Mapping[str, str]:
        """
        Requests the removal of metadata from a payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            keys: List of keys to be removed from the payload metadata.

        Returns:
            A Mapping[str, str] containing the updated set of metadata
        """
        pass


class PayloadsClient(BaseClient, PayloadsClientStub):
    def __init__(self, target: str, port: str = None, stub=None):
        """
        Payloads Client Creation

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
            self._stub = payloads_pb2_grpc.PayloadsStub(self._channel)
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
            self._stub = payloads_pb2_grpc.PayloadsStub(self._channel)
        else:
            print("Connection for client already open")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._channel is not None:
            self.close()
        return False

    def create_payload(self, metadata: Mapping[str, str] = None, timeout=None) -> payload_types.PayloadDetails:
        """
        Creates a static payload.

        Payloads created using this API are created with a type of "PayloadType.Reusable"

        Returns:
             the details of newly created payload.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = payloads_pb2.PayloadsCreateRequest(header=self.get_request_header())

        if metadata is not None:
            request.metadata.update(metadata)

        response = self._stub.Create(request, timeout=timeout)

        self.check_response_header(header=response.header)

        payload_details = payload_types.PayloadDetails(
            file_details=[],
            payload_id=payload_types.PayloadId(response.payload_id.value),
            payload_type=response.type
        )

        return payload_details

    def delete_payload(self, payload_id: payload_types.PayloadId, timeout=None):
        """
        Requests the deletion of a payload, identified by "payload_id" from Clara.

        Deleted payloads cannot be recovered.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload to delete.
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (payload_id.value is None) or (payload_id.value == ""):
            raise Exception("Payload identifier argument must be initialized with non-null instance")

        request = payloads_pb2.PayloadsDeleteRequest(
            header=self.get_request_header(),
            payload_id=payload_id.to_grpc_value()
        )

        response = self._stub.Delete(request, timeout=timeout)

        self.check_response_header(header=response.header)

    def get_details(self, payload_id: payload_types.PayloadId, timeout=None) -> payload_types.PayloadDetails:
        """
        Requests the details of a payload, identified by "payload_id" from Clara.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
        Returns:
            A payload_types.PayloadDetails instance containing payload details
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (payload_id.value is None) or (payload_id.value == ""):
            raise Exception("Payload identifier argument must be initialized with non-null instance")

        request = payloads_pb2.PayloadsDetailsRequest(
            header=self.get_request_header(),
            payload_id=payload_id.to_grpc_value()
        )

        response = self._stub.Details(request, timeout=timeout)

        responses = [resp for resp in response]

        if len(responses) > 0:

            self.check_response_header(header=responses[0].header)

            file_details = []

            for item in responses:

                if item.file is None:
                    continue

                detail = payload_types.PayloadFileDetails(
                    mode=item.file.mode,
                    name=item.file.name,
                    size=item.file.size
                )

                file_details.append(detail)

            result = payload_types.PayloadDetails(
                payload_id=payload_types.PayloadId(responses[0].payload_id.value),
                file_details=file_details,
                payload_type=responses[0].type,
                metadata=responses[0].metadata
            )

            return result

        return None

    def download_from(self, payload_id: payload_types.PayloadId, blob_name: str, dest_obj: BinaryIO = None,
                      dest_path: str = None, timeout=None) -> payload_types.PayloadFileDetails:
        """
        Downloads a blob, identified by "blob_name", from a payload, identified by its "payload_id", from Clara.

        Data downloaded from the payload will be written to 'dest_obj' a passed in BinaryIO object with write privileges

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            blob_name (str): The name, or path, of the blob in the payload.
            dest_obj (BinaryIO): Target stream object to write to with write privileges
            dest_path (str): Alternative to passing in BinaryIO object to download to, and rather passing in path for a file
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (payload_id.value is None) or (payload_id.value == ""):
            raise Exception("Payload identifier argument must be initialized with non-null instance")

        if (blob_name is None) or (blob_name == ""):
            raise Exception("Name of source blob must be initialized with non-null string")

        file_path_used = False

        if dest_obj is None:
            if dest_path is None:
                raise Exception("Destination object for upload must be initialized with non-null BinaryIO object")
            else:
                dest_obj = open(dest_path, 'wb')
                file_path_used = True

        request = payloads_pb2.PayloadsDownloadRequest(
            header=self.get_request_header(),
            name=blob_name,
            payload_id=payload_id.to_grpc_value()
        )

        responses = self._stub.Download(request, timeout=timeout)

        result = None

        for resp in responses:
            if result is None:
                self.check_response_header(header=resp.header)

                result = payload_types.PayloadFileDetails(
                    mode=resp.details.mode,
                    name=resp.details.name,
                    size=resp.details.size
                )

            dest_obj.write(resp.data)

        if file_path_used:
            dest_obj.close()

        return result

    def remove_from(self, payload_id: payload_types.PayloadId, blob_name: str, timeout=None):
        """
        Removes a blob from the payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload from which to remove the blob.
            blob_name (str): The name, or path, of the blob in the payload.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (payload_id.value is None) or (payload_id.value == ""):
            raise Exception("Payload identifier argument must be initialized with non-null instance")

        if (blob_name is None) or (blob_name == ""):
            raise Exception("Name of blob to remove must be initialized with non-null string")

        request = payloads_pb2.PayloadsRemoveRequest(
            header=self.get_request_header(),
            name=blob_name,
            payload_id=payload_id.to_grpc_value()
        )

        response = self._stub.Remove(request, timeout=timeout)

        self.check_response_header(header=response.header)

    def upload_request_iterator(self, payload_id: payload_types.PayloadId, file_name: str,
                                source_object: BinaryIO = None, mode: int = 0):
        """
        Creates generator with data from input file (specified by file_name)

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            file_name (str): File_name to read from
            source_object (BinaryIO): Stream to read from
            mode (int): Privilege level
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if source_object is None:
            raise Exception("Source object must be initialized with a non-null BinaryIO instance")

        chunk_size = 64 * 1024

        while True:
            data = source_object.read(chunk_size)

            if not data:
                return

            details = payloads_pb2.PayloadFileDetails(mode=mode, name=file_name, size=len(data))
            request = payloads_pb2.PayloadsUploadRequest(
                header=self.get_request_header(),
                payload_id=payload_id.to_grpc_value(),
                details=details,
                data=data
            )

            yield request

    def upload(self, payload_id: payload_types.PayloadId, blob_name: str, file_object: BinaryIO = None,
               file_path: str = None,
               timeout=None) -> payload_types.PayloadFileDetails:
        """
        Uploads a blob from "file_object", to a Clara Payload identified by "payload_id".

        Each uploaded blob must be have a unique "blob_name" value within a given payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            blob_name (str): The name, or path, of the blob in the payload.
            file_object (BinaryIO): stream to read from and upload with read privileges
            file_path (str): Alternative to passing in BinaryIO object for upload, and rather passing in path for a file
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (payload_id.value is None) or (payload_id.value == ""):
            raise Exception("Payload identifier argument must be initialized with non-null instance")

        if (blob_name is None) or (blob_name == ""):
            raise Exception("Name of destination blob must be initialized with non-null string")

        file_path_used = False

        if file_object is None:
            if file_path is None:
                raise Exception("File_object of file for upload must be initialized with non-null BinaryIO object")
            else:
                file_object = open(file_path, 'rb')
                file_path_used = True

        requests = self.upload_request_iterator(
            payload_id=payload_id,
            file_name=blob_name,
            source_object=file_object
        )

        response = self._stub.Upload(
            requests,
            timeout=timeout
        )

        self.check_response_header(header=response.header)

        result = payload_types.PayloadFileDetails(other=response.details)

        if file_path_used:
            file_object.close()

        return result

    def add_metadata(self, payload_id: payload_types.PayloadId, metadata: Mapping[str, str], timeout=None) -> Mapping[
        str, str]:
        """
        Requests the addition of metadata to a payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the payload metadata. If a metadata
                    key in the request already exists in the job record, or if duplicate keys are passed in the request,
                    the payload will not be updated and and an error will be returned. Keys are compared using case
                    insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes, while
                    the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the overall
                    metadata of an individual payload is 4 Megabytes.

        Returns:
            A Mapping[str, str] containing the appended metadata
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (payload_id.value is None) or (payload_id.value == ""):
            raise Exception("Payload identifier must have instantiated value")

        if metadata is None:
            raise Exception("Metadata must be an instantiated map")

        request = payloads_pb2.PayloadsAddMetadataRequest(
            payload_id=payload_id.to_grpc_value()
        )

        request.metadata.update(metadata)

        response = self._stub.AddMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result

    def remove_metadata(self, payload_id: payload_types.PayloadId, keys: List[str], timeout=None) -> Mapping[str, str]:
        """
        Requests the removal of metadata from a payload.

        Args:
            payload_id (payload_types.PayloadId): Unique identifier of the payload.
            keys: List of keys to be removed from the payload metadata.

        Returns:
            A Mapping[str, str] containing the updated set of metadata
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (payload_id.value is None) or (payload_id.value == ""):
            raise Exception("Payload identifier must have instantiated value")

        if keys is None:
            raise Exception("Keys paramater must be valid list of metadata keys")

        request = payloads_pb2.PayloadsRemoveMetadataRequest(
            payload_id=payload_id.to_grpc_value()
        )

        request.keys.extend(keys)

        response = self._stub.RemoveMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result
