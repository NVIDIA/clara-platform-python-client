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
from typing import Mapping as HashMap, List, Mapping
from nvidia_clara.grpc import models_pb2, common_pb2


class CatalogId:

    def __init__(self, value: str):
        """
        Unique identifier for an Inference Model Catalog.
        """

        if (value is None) or (value == ""):
            raise Exception("Catalog identifier value must be intialized.")

        self._value = value

    @property
    def value(self):
        return self._value

    def _eq_(self, other) -> bool:
        return self._value == other.value

    def _ne_(self, other) -> bool:
        return not (self == other)

    def _repr_(self):
        return "%s" % self._value

    def _str_(self):
        return "%s" % self._value

    def _hash_(self):
        return hash(self._value)

    def to_grpc_value(self):
        id = common_pb2.Identifier()
        id.value = self._value
        return id


class ModelType(Enum):
    Unknown = 0

    TensorFlow = 1

    TensorRT = 2

    PyTorch = 3

    Minimum = TensorFlow

    Maximum = PyTorch


class ModelId:
    def __init__(self, value: str):
        if (value is None) or (value == ""):
            raise Exception("Model identifier value must be intialized.")

        self._value = value

    @property
    def value(self):
        return self._value

    def _eq_(self, other) -> bool:
        return self._value == other.value

    def _ne_(self, other) -> bool:
        return not (self == other)

    def _repr_(self):
        return "%s" % self._value

    def _str_(self):
        return "%s" % self._value

    def _hash_(self):
        return hash(self._value)

    def to_grpc_value(self):
        id = common_pb2.Identifier()
        id.value = self._value
        return id


class ModelDetails:

    def __init__(self, other: models_pb2.ModelDetails = None, model_id: ModelId = None, name: str = None,
                 tags: Mapping[str, str] = None,
                 model_type: ModelType = None, metadata: Mapping[str, str] = None):
        if other is None:
            if tags is None:
                tags = dict()
            if metadata is None:
                metadata = dict()
            self._model_id = model_id
            self._name = name
            self._tags = tags
            self._model_type = model_type
            self._metadata = metadata
        else:
            self._model_id = other.model_id
            self._name = other.name
            self._tags = other.tags
            self._model_type = other.type
            self._metadata = other.metadata

    @property
    def model_id(self) -> ModelId:
        """Unique identifier of this inference model."""
        return self._model_id

    @model_id.setter
    def model_id(self, model_id: ModelId = None):
        """Unique identifier of this inference model."""
        self._model_id = model_id

    @property
    def name(self) -> str:
        """The name of this inference model."""
        return self._name

    @name.setter
    def name(self, name: str = None):
        """The name of this inference model."""
        self._name = name

    @property
    def tags(self) -> Mapping[str, str]:
        """The set of tags / meta-data associated with this infrence model."""
        return self._tags

    @tags.setter
    def tags(self, tags: Mapping[str, str] = None):
        """The set of tags / meta-data associated with this infrence model."""
        self._tags = tags

    @property
    def model_type(self) -> ModelId:
        """The type (inference toolset) of this inference model."""
        return self._model_type

    @model_type.setter
    def model_type(self, model_type: ModelType = None):
        """The type (inference toolset) of this inference model."""
        self._model_type = model_type

    @property
    def metadata(self) -> Mapping[str, str]:
        """
        Metadata (set of key/value pairs) associated with the model
        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Mapping[str, str]):
        """
        Metadata (set of key/value pairs) associated with the model
        """
        self._metadata = metadata


class CatalogDetails:

    def __init__(self, other: models_pb2.ModelCatalogDetails = None, catalog_id: CatalogId = None,
                 models: List[ModelDetails] = None):
        if other is None:
            if catalog_id is None:
                raise Exception("Catalog identifier can not be None and must be initializes")

            self._catalog_id = catalog_id

            if models is None:
                self._models = []
            else:
                self._models = models
        else:
            self._catalog_id = None

            if (other.catalog_id.value is not None) or (other.catalog_id.value != ""):
                self._catalog_id = CatalogId(value=other.catalog_id.value)

            self._models = []

            if len(other.models) > 0:

                for model in other.models:
                    new_model = ModelDetails(other=model)
                    self._models.append(new_model)

    @property
    def catalog_id(self) -> CatalogId:
        """Unique identifier of this inference model catalog."""
        return self._catalog_id

    @catalog_id.setter
    def catalog_id(self, catalog_id: CatalogId = None):
        """Unique identifier of this inference model catalog."""
        self._catalog_id = catalog_id

    @property
    def models(self) -> List[ModelDetails]:
        """List of inference models associated with this inference model catalog."""
        return self._models

    @models.setter
    def models(self, models: List[ModelDetails] = None):
        """List of inference models associated with this inference model catalog."""
        self._models = models


class InstanceId:
    def __init__(self, value: str):
        """Unique identifier for an Model Catalog Instance."""
        if (value is None) or (value == ""):
            raise Exception("InstanceId identifier value must be intialized.")

        self._value = value

    @property
    def value(self):
        return self._value

    def _eq_(self, other) -> bool:
        return self._value == other.value

    def _ne_(self, other) -> bool:
        return not (self == other)

    def _repr_(self):
        return "%s" % self._value

    def _str_(self):
        return "%s" % self._value

    def _hash_(self):
        return hash(self._value)

    def to_grpc_value(self):
        id = common_pb2.Identifier()
        id.value = self._value
        return id


class InstanceDetails:
    def __init__(self, other: models_pb2.ModelCatalogDetails = None, instance_id: InstanceId = None,
                 models: List[ModelDetails] = None):
        if other is None:
            if instance_id is None:
                raise Exception("Instance identifier can not be None and must be initializes")

            self._instance_id = instance_id

            if models is None:
                self._models = []
            else:
                self._models = models
        else:
            self._instance_id = None

            if (other.catalog_id.value is not None) or (other.catalog_id.value != ""):
                self._instance_id = InstanceId(value=other.catalog_id.value)

            self._models = []

            if len(other.models) > 0:

                for model in other.models:
                    new_model = ModelDetails(other=model)
                    self._models.append(new_model)

    @property
    def instance_id(self) -> InstanceId:
        """Unqiue identifier of this inference model catalog instance."""
        return self._instance_id

    @instance_id.setter
    def instance_id(self, instance_id: InstanceId = None):
        """Unqiue identifier of this inference model catalog instance."""
        self._instance_id = instance_id

    @property
    def models(self) -> List[ModelDetails]:
        """List of inference models associated with this inference model catalog instance."""
        return self._models

    @models.setter
    def models(self, models: List[ModelDetails] = None):
        """List of inference models associated with this inference model catalog instance."""
        self._models = models
