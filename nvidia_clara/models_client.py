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

from typing import BinaryIO, List, Mapping
import grpc
from nvidia_clara.grpc import models_pb2, models_pb2_grpc
from nvidia_clara.base_client import BaseClient
import nvidia_clara.model_types as model_types


class ModelsClientStub:
    def create_catalog(self) -> model_types.CatalogId:
        """
        Creates a new inference model catalog.

        Returns:
            model_types.CatalogId of the unique identity of the new catalog.
        """
        pass

    def create_instance(self) -> model_types.InstanceId:
        """
        Creates a new inference model catalog instance.

        Returns:
            model_types.InstanceId of the unique identity of the new instance.
        """
        pass

    def delete_catalog(self, catalog_id: model_types.CatalogId):
        """
        Deletes the inference catalog associated with "catalog_id"

        Deleted catalogs can no longer be used by Clara Platform Server to manage inference server model collections.

        Deleted catalogs cannot be recovered.

        Args:
            catalog_id (model_types.CatalogId): Unique identifier for the inference model catalog to be deleted
        """
        pass

    def delete_instance(self, instance_id: model_types.InstanceId):
        """
        Deletes the inference model catalog instance associated with "instance_id"

        Deleted instances can no longer be used by Clara Platform Server to manage inference server model collections.

        Deleted instances cannot be recovered.

        Args:
            instance_id (model_types.InstanceId): Unique identifier for the inference model catalog instance to be deleted.
        """
        pass

    def delete_model(self, model_id: model_types.ModelId):
        """
        Deletes the nference model associated with

        Deleted models cannot be used by pipeline jobs for inference.

        Deleted models cannot be recovered.

        Args:
            model_id (model_types.ModelId): Unique identifier of the inference model to be deleted.
        """
        pass

    def download_model(self, model_id: model_types.ModelId, output_stream: BinaryIO) -> model_types.ModelDetails:
        """
        Downloads the model associated with "model_id" to an "output_stream" BinaryIO object

        Args:
            model_id (model_types.ModelId): Unique identifier of the model to download.
            output_stream (BinaryIO): Writable stream use to write the raw model data to.

        Returns:
            model_types.ModelDetails with details of the downloaded model.
        """
        pass

    def list_models(self) -> List[model_types.ModelDetails]:
        """
        Returns details of all inference models known to the server.

        Only inference model details are provided; no model raw data is downloaded.

        Returns:
            List[model_types.ModelDetails] with each element containing details of all inference models known to the server
        """
        pass

    def read_catalog(self, catalog_id: model_types.CatalogId) -> List[model_types.ModelDetails]:
        """
        Returns details of all inference models included in the catalog associated with "catalog_id"

        Args:
            catalog_id (model_types.CatalogId): Unique identifier of the inference catalog to read.

        Returns:
            List[model_types.ModelDetails] ith each element containing details of all inference models associated with catalog
        """
        pass

    def read_instance(self, instance_id: model_types.InstanceId) -> List[model_types.ModelDetails]:
        """
        Returns details of all inference models included in the catalog instance associated with "instance_id"

        Args:
            instance_id (model_types.InstanceId): Unique identifier of the inference catalog instance to read.

        Returns:
            List[model_types.ModelDetails] ith each element containing details of all inference models associated with Instance
        """
        pass

    def update_catalog(self, catalog_id: model_types.CatalogId, model_ids: List[model_types.ModelId]):
        """
        Updates the inference model catalog associated with "catalog_id" and sets its set of included models in "model_ids"

        Any existing list of inference models associated with the catalog is replaced with the new list.

        Args:
            catalog_id (model_types.CatalogId): Unique identifier of the inference model catalog to update.
            model_ids: List of inference model identifiers to replace any existing list with.
        """
        pass

    def update_instance(self, instance_id: model_types.InstanceId, model_ids: List[model_types.ModelId]):
        """
        Updates the inference model catalog instance associated with "instance_id" and sets its set of included models to "model_ids"

        Any existing list of inference models associated with the instance is replaced with the new list.

        Args:
            instance_id (model_types.InstanceId): Unique identifier of the inference model catalog instance to update.
            model_ids: List of inference model identifiers to replace any existing list with.
        """
        pass

    def upload_model(self, details: model_types.ModelDetails, input_stream: BinaryIO):
        """
        Uploads an inference model to the model repository.

        If a model with the same name exists, it will be overwritten by this operation.

        Args:
            details (model_types.ModelDetails): provides details, including the name of the model.
            input_stream (BinaryIO): Raw model data is read from this stream and persisted into storage by the model repository.
        """
        pass

    def add_metadata(self, model_id: model_types.ModelId, metadata: Mapping[str, str]) -> Mapping[str, str]:
        """
        Requests the addition of metadata to a model.

        Args:
            model_id (model_types.ModelId): Unique identifier of the model to download.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the job metadata. If a metadata
                    key in the request already exists in the model record, or if duplicate keys are passed in the request,
                    the model will not be updated and and an error will be returned. Keys are compared using case
                    insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes, while
                    the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the overall
                    metadata of an individual model is 4 Megabytes.

        Returns:
            A Mapping[str, str] containing the appended metadata
        """
        pass

    def remove_metadata(self, model_id: model_types.ModelId, keys: List[str]) -> Mapping[str, str]:
        """
        Requests the removal of metadata from a model.

        Args:
            model_id (model_types.ModelId): Unique identifier of the model to download.
            keys: List of keys to be removed from the model metadata.

        Returns:
            A Mapping[str, str] containing the updated set of metadata
        """
        pass


class ModelsClient(ModelsClientStub, BaseClient):

    def __init__(self, target: str, port: str = None, stub=None):
        """
        Models Client Creation

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
            self._stub = models_pb2_grpc.ModelsStub(self._channel)
        else:
            self._stub = stub

    def close(self):
        """Close connection"""
        if self._channel:
            self._channel.close()
            self._channel = None
            self._stub = None
        else:
            print("Connection for client already closed")

    def reconnect(self):
        """Re-open connection with existing channel"""
        if self._channel is None:
            self._channel = grpc.insecure_channel(self._connection)
            self._stub = models_pb2_grpc.ModelsStub(self._channel)
        else:
            print("Connection for client already open")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._channel is not None:
            self.close()
        return False

    def create_catalog(self, timeout=None) -> model_types.CatalogId:
        """
        Creates a new inference model catalog.

        Returns:
            model_types.CatalogId of the unique identity of the new catalog.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = models_pb2.ModelsCreateCatalogRequest(
            header=self.get_request_header()
        )

        response = self._stub.CreateCatalog(request, timeout=timeout)

        self.check_response_header(header=response.header)

        result = model_types.CatalogId(value=response.catalog_id.value)

        return result

    def create_instance(self, timeout=None) -> model_types.InstanceId:
        """
        Creates a new inference model catalog instance.

        Returns:
            model_types.InstanceId of the unique identity of the new instance.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = models_pb2.ModelsCreateInstanceRequest(
            header=self.get_request_header()
        )

        response = self._stub.CreateInstance(request, timeout=timeout)

        self.check_response_header(header=response.header)

        result = model_types.InstanceId(value=response.instance_id.value)

        return result

    def delete_catalog(self, catalog_id: model_types.CatalogId, timeout=None):
        """
        Deletes the inference catalog associated with "catalog_id"

        Deleted catalogs can no longer be used by Clara Platform Server to manage inference server model collections.

        Deleted catalogs cannot be recovered.

        Args:
            catalog_id (model_types.CatalogId): Unique identifier for the inference model catalog to be deleted
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (catalog_id.value is None) or (catalog_id.value == ""):
            raise Exception("Catalog identifier must be initialized to non-null instance of model_types.CatalogId")

        request = models_pb2.ModelsDeleteCatalogRequest(
            catalog_id=catalog_id.to_grpc_value(),
            header=self.get_request_header()
        )

        response = self._stub.DeleteCatalog(request, timeout=timeout)

        self.check_response_header(header=response.header)

    def delete_instance(self, instance_id: model_types.InstanceId, timeout=None):
        """
        Deletes the inference model catalog instance associated with "instance_id"

        Deleted instances can no longer be used by Clara Platform Server to manage inference server model collections.

        Deleted instances cannot be recovered.

        Args:
            instance_id (model_types.InstanceId): Unique identifier for the inference model catalog instance to be deleted.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if self._channel is None:
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (instance_id.value is None) or (instance_id.value == ""):
            raise Exception("Instance identifier must be initialized to non-null instance of model_types.InstanceId")

        request = models_pb2.ModelsDeleteInstanceRequest(
            catalog_id=instance_id.to_grpc_value(),
            header=self.get_request_header()
        )

        response = self._stub.DeleteInstance(request, timeout=timeout)

        self.check_response_header(header=response.header)

    def delete_model(self, model_id: model_types.ModelId, timeout=None):
        """
        Deletes the nference model associated with

        Deleted models cannot be used by pipeline jobs for inference.

        Deleted models cannot be recovered.

        Args:
            model_id (model_types.ModelId): Unique identifier of the inference model to be deleted.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (model_id.value is None) or (model_id.value == ""):
            raise Exception("Model identifier must be initialized to non-null instance of model_types.ModelId")

        request = models_pb2.ModelsDeleteModelRequest(
            catalog_id=model_id.to_grpc_value(),
            header=self.get_request_header()
        )

        response = self._stub.DeleteModel(request, timeout=timeout)

        self.check_response_header(header=response.header)

    def download_model(self, model_id: model_types.ModelId, output_stream: BinaryIO,
                       timeout=None) -> model_types.ModelDetails:
        """
        Downloads the model associated with "model_id" to an "output_stream" BinaryIO object

        Args:
            model_id (model_types.ModelId): Unique identifier of the model to download.
            output_stream (BinaryIO): Writable stream use to write the raw model data to.

        Returns:
            model_types.ModelDetails with details of the downloaded model.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (model_id.value is None) or (model_id.value == ""):
            raise Exception("Model identifier must be initialized to non-null instance of model_types.ModelId")

        request = models_pb2.ModelsDownloadModelRequest(
            header=self.get_request_header(),
            model_id=model_id.to_grpc_value()
        )

        responses = self._stub.Download(request, timeout=timeout)

        result = None

        for resp in responses:
            if result is None:
                self.check_response_header(header=resp.header)

                result = model_types.ModelDetails(other=resp.details)

            output_stream.write(resp.data)

        return result

    def list_models(self, timeout=None) -> List[model_types.ModelDetails]:
        """
        Returns details of all inference models known to the server.

        Only inference model details are provided; no model raw data is downloaded.

        Returns:
            List[model_types.ModelDetails] with each element containing details of all inference models known to the server
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = models_pb2.ModelsListModelsRequest(
            header=self.get_request_header()
        )

        response = self._stub.ListModels(request, timeout=timeout)

        responses = [resp for resp in response]

        if len(responses) > 0:

            self.check_response_header(header=responses[0].header)

            result = []

            for resp in responses:
                details = model_types.ModelDetails(other=resp.models)
                result.append(details)

            return result

        return None

    def read_catalog(self, catalog_id: model_types.CatalogId, timeout=None) -> List[model_types.ModelDetails]:
        """
        Returns details of all inference models included in the catalog associated with "catalog_id"

        Args:
            catalog_id (model_types.CatalogId): Unique identifier of the inference catalog to read.

        Returns:
            List[model_types.ModelDetails] ith each element containing details of all inference models associated with catalog
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = models_pb2.ModelsReadCatalogRequest(
            catalog_id=catalog_id.to_grpc_value(),
            header=self.get_request_header()
        )

        response = self._stub.ReadCatalog(request, timeout=timeout)

        responses = [resp for resp in response]

        if len(responses) > 0:

            self.check_response_header(header=responses[0].header)

            result = []

            for resp in responses:
                details = model_types.ModelDetails(other=resp.models)
                result.append(details)

            return result

        return None

    def read_instance(self, instance_id: model_types.InstanceId, timeout=None) -> List[model_types.ModelDetails]:
        """
        Returns details of all inference models included in the catalog instance associated with "instance_id"

        Args:
            instance_id (model_types.InstanceId): Unique identifier of the inference catalog instance to read.

        Returns:
            List[model_types.ModelDetails] ith each element containing details of all inference models associated with Instance
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = models_pb2.ModelsReadCatalogRequest(
            instance_id=instance_id.to_grpc_value(),
            header=self.get_request_header()
        )

        response = self._stub.ReadInstance(request, timeout=timeout)

        responses = [resp for resp in response]

        if len(responses) > 0:

            self.check_response_header(header=responses[0].header)

            result = []

            for resp in responses:
                details = model_types.ModelDetails(other=resp.models)
                result.append(details)

            return result

        return None

    def update_catalog(self, catalog_id: model_types.CatalogId, model_ids: List[model_types.ModelId], timeout=None):
        """
        Updates the inference model catalog associated with "catalog_id" and sets its set of included models in "model_ids"

        Any existing list of inference models associated with the catalog is replaced with the new list.

        Args:
            catalog_id (model_types.CatalogId): Unique identifier of the inference model catalog to update.
            model_ids: List of inference model identifiers to replace any existing list with.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        model_ids_req = [model_id.to_grpc_value() for model_id in model_ids]

        request = models_pb2.ModelsUpdateCatalogRequest(
            catalog_id=catalog_id.to_grpc_value(),
            header=self.get_request_header(),
            model_ids=model_ids_req
        )

        response = self._stub.UpdateCatalog([request], timeout=timeout)

        self.check_response_header(header=response.header)

    def update_instance(self, instance_id: model_types.InstanceId, model_ids: List[model_types.ModelId], timeout=None):
        """
        Updates the inference model catalog instance associated with "instance_id" and sets its set of included models to "model_ids"

        Any existing list of inference models associated with the instance is replaced with the new list.

        Args:
            instance_id (model_types.InstanceId): Unique identifier of the inference model catalog instance to update.
            model_ids: List of inference model identifiers to replace any existing list with.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        model_ids_req = [model_id.to_grpc_value() for model_id in model_ids]

        request = models_pb2.ModelsReadInstanceRequest(
            instance_id=instance_id.to_grpc_value(),
            header=self.get_request_header(),
            model_ids=model_ids_req
        )

        response = self._stub.UpdateInstance([request], timeout=timeout)

        self.check_response_header(header=response.header)

    def upload_request_iterator(self, details: models_pb2.ModelDetails, source_object: BinaryIO = None):
        """
        Helper method for uplaod model that creates generator of requests

        Args:
            details (models_pb2.ModelDetails): details of specified model
            source_object (BinaryIO): model source file to read data from
        """

        if source_object is None:
            raise Exception("Source object must be initialized with a non-null BinaryIO instance")

        chunk_size = 64 * 1024

        while True:
            data = source_object.read(chunk_size)

            if not data:
                return

            request = models_pb2.ModelsUploadModelRequest(
                header=self.get_request_header(),
                details=details,
                data=data
            )

            yield request

    def upload_model(self, details: model_types.ModelDetails, input_stream: BinaryIO, timeout=None):
        """
        Uploads an inference model to the model repository.

        If a model with the same name exists, it will be overwritten by this operation.

        Args:
            details (model_types.ModelDetails): provides details, including the name of the model.
            input_stream (BinaryIO): Raw model data is read from this stream and persisted into storage by the model repository.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        details = models_pb2.ModelDetails(model_id=details.model_id.to_grpc_value(), name=details.name,
                                          tags=details.tags, model_type=details.model_type)

        response = self._stub.UploadModel(
            self.upload_request_iterator(details=details, source_object=input_stream),
            timeout=timeout
        )

        self.check_response_header(header=response.header)

    def add_metadata(self, model_id: model_types.ModelId, metadata: Mapping[str, str], timeout=None) -> Mapping[
        str, str]:
        """
        Requests the addition of metadata to a model.

        Args:
            model_id (model_types.ModelId): Unique identifier of the model to download.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the job metadata. If a metadata
                    key in the request already exists in the model record, or if duplicate keys are passed in the request,
                    the model will not be updated and and an error will be returned. Keys are compared using case
                    insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes, while
                    the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the overall
                    metadata of an individual model is 4 Megabytes.

        Returns:
            A Mapping[str, str] containing the appended metadata
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (model_id.value is None) or (model_id.value == ""):
            raise Exception("Model identifier must have instantiated value")

        if metadata is None:
            raise Exception("Metadata must be an instantiated map")

        request = models_pb2.ModelsAddMetadataRequest(
            model_id=model_id.to_grpc_value()
        )

        request.metadata.update(metadata)

        response = self._stub.AddMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result

    def remove_metadata(self, model_id: model_types.ModelId, keys: List[str], timeout=None) -> Mapping[str, str]:
        """
        Requests the removal of metadata from a model.

        Args:
            model_id (model_types.ModelId): Unique identifier of the model to download.
            keys: List of keys to be removed from the model metadata.

        Returns:
            A Mapping[str, str] containing the updated set of metadata
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (model_id.value is None) or (model_id.value == ""):
            raise Exception("Model identifier must have instantiated value")

        if keys is None:
            raise Exception("Keys paramater must be valid list of metadata keys")

        request = models_pb2.ModelsRemoveMetadataRequest(
            model_id=model_id.to_grpc_value()
        )

        request.keys.extend(keys)

        response = self._stub.RemoveMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result
