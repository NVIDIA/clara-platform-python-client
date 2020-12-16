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

import datetime
from typing import List, Mapping, Iterator
import grpc
from nvidia_clara.grpc import common_pb2, clara_pb2, clara_pb2_grpc
from nvidia_clara.base_client import BaseClient
import nvidia_clara.clara_types as clara_types
import nvidia_clara.job_types as job_types


class ClaraClient(BaseClient):

    def __init__(self, target: str, port: str = None, stub=None):
        """
        Clara Client Creation

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
            self._stub = clara_pb2_grpc.ClaraStub(self._channel)
        else:
            self._stub = stub

    @staticmethod
    def get_timestamp(seconds_since_year_one: str) -> datetime.datetime:
        """
        Create datetime.datetime object from a string date

        Args:
            seconds_since_year_one(str): date to parse

        Returns:
            datetime.datetime object
        """
        if (seconds_since_year_one is None) or (seconds_since_year_one == ""):
            return None

        try:
            # Check to see if in form of seconds since year one
            seconds_int = float(seconds_since_year_one.value) - 62167219200
        except:
            # Otherwise parse timestamp
            return datetime.datetime.strptime(seconds_since_year_one, "%Y-%m-%d %H:%M:%SZ")

        if seconds_int < 0:
            return None

        result_date = datetime.datetime.fromtimestamp(seconds_int)

        return result_date

    def stop(self, timeout=None):
        """Sends stop request to instance of Pipeline Services and Triton"""

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = clara_pb2.ClaraStopRequest(header=self.get_request_header())

        response = self._stub.Stop(request, timeout=timeout)

        self.check_response_header(header=response.header)

    def list_utilization(self, timeout=None) -> List[clara_types.ClaraUtilizationDetails]:
        """
        Method for aquiring snapshot of GPU utilization information of Clara in a list
        
        Returns:
            List[clara_types.ClaraGpuUtilization] with snapshot of GPU Utilization details for Clara GPUs
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = clara_pb2.ClaraUtilizationRequest(header=self.get_request_header(), watch=False)

        response = self._stub.Utilization(request, timeout=timeout)

        utilization_list = []

        header_check = False

        for resp in response:

            if not header_check:
                self.check_response_header(header=resp.header)
                header_check = True

            metrics = resp.gpu_metrics
            clara_utilization_details = clara_types.ClaraUtilizationDetails()
            for item in metrics:
                gpu_utilization = clara_types.ClaraGpuUtilization(
                    node_id=item.node_id,
                    pcie_id=item.pcie_id,
                    compute_utilization=item.compute_utilization,
                    memory_free=item.memory_free,
                    memory_used=item.memory_used,
                    memory_utilization=item.memory_utilization,
                    timestamp=self.get_timestamp(item.timestamp),
                )

                for proc_info in item.process_details:
                    process_details = clara_types.ClaraProcessDetails(
                        name=proc_info.name,
                        job_id=job_types.JobId(proc_info.job_id.value)
                    )
                    gpu_utilization.process_details.append((process_details))

                clara_utilization_details.gpu_metrics.append((gpu_utilization))

            utilization_list.append(clara_utilization_details)

        return utilization_list

    def stream_utilization(self, timeout=None) -> Iterator[clara_types.ClaraUtilizationDetails]:
        """
        Method for aquiring stream of GPU utilization information of Clara

        Returns:
            Iterator[clara_types.ClaraUtilizationDetails] with stream of GPU Utilization details for Clara GPUs
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        request = clara_pb2.ClaraUtilizationRequest(header=self.get_request_header(), watch=True)

        response = self._stub.Utilization(request, timeout=timeout)

        header_check = False

        for resp in response:

            if not header_check:
                self.check_response_header(header=resp.header)
                header_check = True

            metrics = resp.gpu_metrics
            clara_utilization_details = clara_types.ClaraUtilizationDetails()
            for item in metrics:
                gpu_utilization = clara_types.ClaraGpuUtilization(
                    node_id=item.node_id,
                    pcie_id=item.pcie_id,
                    compute_utilization=item.compute_utilization,
                    memory_free=item.memory_free,
                    memory_used=item.memory_used,
                    memory_utilization=item.memory_utilization,
                    timestamp=self.get_timestamp(item.timestamp),
                )

                for proc_info in item.process_details:
                    process_details = clara_types.ClaraProcessDetails(
                        name=proc_info.name,
                        job_id=job_types.JobId(proc_info.job_id.value)
                    )
                    gpu_utilization.process_details.append((process_details))

                clara_utilization_details.gpu_metrics.append((gpu_utilization))

            yield clara_utilization_details

    def version(self, timeout=None):
        """Get Clara Version"""

        request = clara_pb2.ClaraVersionRequest(header=self.get_request_header())

        response = self._stub.Version(request, timeout=timeout)

        self.check_response_header(header=response.header)

        result = clara_types.ClaraVersionInfo(
            major=response.version.major,
            minor=response.version.minor,
            patch=response.version.patch,
            label=response.version.label
        )

        return result
