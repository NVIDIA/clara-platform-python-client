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

from datetime import datetime
from typing import List

from nvidia_clara.job_types import JobId


class ClaraVersionInfo:

    def __init__(self, major: int = None, minor: int = None, patch: int = None, label: str = None):
        """Clara version information."""
        self._major = major
        self._minor = minor
        self._patch = patch
        self._label = label

    @property
    def major(self) -> int:
        """Version Major"""
        return self._major

    @major.setter
    def major(self, major: int):
        """Version Major"""
        self._major = major

    @property
    def minor(self) -> int:
        """Version Minor"""
        return self._minor

    @minor.setter
    def minor(self, minor: int):
        """Version Minor"""
        self._minor = minor

    @property
    def patch(self) -> int:
        """Version Patch"""
        return self._patch

    @patch.setter
    def patch(self, patch: int):
        """Version Patch"""
        self._patch = patch

    @property
    def label(self) -> str:
        """Version Label"""
        return self._label

    @label.setter
    def label(self, label: str):
        """Version Label"""
        self._label = label


class ClaraProcessDetails:
    def __init__(self, name: str = None, job_id: JobId = None):
        self._name = name
        self._job_id = job_id

    @property
    def name(self) -> str:
        """
        Name of the process utilizing the GPU.
            - When job_id is provided, is the unique (to the pipeline-job) name of the pipeline-job operator utilizing the GPU.
            - When job_id is not provided, is the name of the Clara Platform managed, non-pipeline process utilizing the GPU.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Name of the process utilizing the GPU.
            - When job_id is provided, is the unique (to the pipeline-job) name of the pipeline-job operator utilizing the GPU.
            - When job_id is not provided, is the name of the Clara Platform managed, non-pipeline process utilizing the GPU.
        """
        self._name = name

    @property
    def job_id(self) -> JobId:
        """Unique identifier of the pipeline-job utilizing the GPU.
        Only provided when the process utilizing the GPU is a pipeline-job.
        """
        return self._job_id

    @job_id.setter
    def job_id(self, job_id: JobId):
        """Unique identifier of the pipeline-job utilizing the GPU.
        Only provided when the process utilizing the GPU is a pipeline-job.
        """
        self._job_id = job_id


class ClaraGpuUtilization:

    def __init__(self, node_id: int = None, pcie_id: int = None, compute_utilization: float = None,
                 memory_free: int = None, memory_used: int = None, memory_utilization: float = None,
                 process_details: List[ClaraProcessDetails] = None, timestamp: datetime = None):
        """GPU Utilization details for a Clara process."""
        if process_details is None:
            process_details = []
        self._node_id = node_id
        self._pcie_id = pcie_id
        self._compute_utilization = compute_utilization
        self._memory_free = memory_free
        self._memory_used = memory_used
        self._memory_utilization = memory_utilization
        self._process_details = process_details
        self._timestamp = timestamp

    @property
    def node_id(self) -> int:
        """Unique (to the cluster) name of the node which contains the GPU."""
        return self._node_id

    @node_id.setter
    def node_id(self, node_id: int):
        """Unique (to the cluster) name of the node which contains the GPU."""
        self._node_id = node_id

    @property
    def pcie_id(self) -> int:
        """PCIE device identifier of the GPU."""
        return self._pcie_id

    @pcie_id.setter
    def pcie_id(self, pcie_id: int):
        """PCIE device identifier of the GPU."""
        self._pcie_id = pcie_id

    @property
    def compute_utilization(self) -> float:
        """GPU compute utilization; in the range of zero to one, inclusive [0, 1]."""
        return self._compute_utilization

    @compute_utilization.setter
    def compute_utilization(self, compute_utilization: float):
        """GPU compute utilization; in the range of zero to one, inclusive [0, 1]."""
        self._compute_utilization = compute_utilization

    @property
    def memory_free(self) -> int:
        """Free GPU memory, measured in megabytes."""
        return self._memory_free

    @memory_free.setter
    def memory_free(self, memory_free: int):
        """Free GPU memory, measured in megabytes."""
        self._memory_free = memory_free

    @property
    def memory_used(self) -> int:
        """Used GPU memory, measured in megabytes."""
        return self._memory_used

    @memory_used.setter
    def memory_used(self, memory_used: int):
        """Used GPU memory, measured in megabytes."""
        self._memory_used = memory_used

    @property
    def memory_utilization(self) -> float:
        """GPU memory utilization; in the range of zero to one, inclusive [0, 1]."""
        return self._memory_utilization

    @memory_utilization.setter
    def memory_utilization(self, memory_utilization: float):
        """GPU memory utilization; in the range of zero to one, inclusive [0, 1]."""
        self._memory_utilization = memory_utilization

    @property
    def process_details(self) -> List[ClaraProcessDetails]:
        """List of pipeline-job operators and/or Clara Platform managed process utilizing the GPU."""
        return self._process_details

    @process_details.setter
    def process_details(self, process_details: List[ClaraProcessDetails]):
        """List of pipeline-job operators and/or Clara Platform managed process utilizing the GPU."""
        self._process_details = process_details

    @property
    def timestamp(self) -> datetime:
        """Timestamp when the associated metrics data was collected."""
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp: datetime):
        """Timestamp when the associated metrics data was collected."""
        self._timestamp = timestamp


class ClaraUtilizationDetails:
    """Utilization details for a Clara process."""

    def __init__(self, gpu_metrics: List[ClaraGpuUtilization] = None):
        if gpu_metrics is None:
            gpu_metrics = []
        self._gpu_metrics = gpu_metrics

    @property
    def gpu_metrics(self) -> List[ClaraGpuUtilization]:
        """List of Utilization Details of each GPU"""
        return self._gpu_metrics

    @gpu_metrics.setter
    def gpu_metrics(self, gpu_metrics: List[ClaraGpuUtilization]):
        """List of Utilization Details of each GPU"""
        self._gpu_metrics = gpu_metrics
