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
from typing import List, Mapping
import grpc
import itertools
from nvidia_clara.grpc import common_pb2, jobs_pb2, jobs_pb2_grpc
from nvidia_clara.base_client import BaseClient
import nvidia_clara.job_types as job_types
import nvidia_clara.pipeline_types as pipeline_types
import nvidia_clara.payload_types as payload_types


class JobsClientStub:

    def cancel_job(self, job_id: job_types.JobId, reason=None) -> job_types.JobToken:
        """
        Cancels a pipeline job, preventing it from being executed.

        Has no affect on executing or terminated jobs.

        Args:
            job_id (job_types.JobId): Unique identity of the job to be cancelled.
            reason: Optional reason as to why the job was cancelled.

        Returns:
            job_types.JobToken of cancelled job
        """
        pass

    def create_job(self, pipeline_id: pipeline_types.PipelineId, job_name: str, job_priority: job_types.JobPriority,
                   input_payloads: List[payload_types.PayloadId] = None,
                   metadata: Mapping[str, str] = None) -> job_types.JobInfo:
        """
        Creates a new pipeline job record and associate storage payload.

        Jobs are created in a "JobState.Pending" state.

        Use "StartJob(JobId, Map{KeyValuePair{string, string}}" to cause the job to start executing.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the pipeline which the job should
                be instances from.
            job_name (str): Human readable name of the job.
            job_priority (job_types.JobPriority): Optional Priority of the job.
                Affects how and when the server will schedule the job.
            input_payloads (List[payload_types.PayloadId]): [Optional Paramater] List of static payloads to
                include as input for the job.
            metadata (Mapping[str, str]): [Optional Parameter] Metadata (set of key/value pairs) associated with the
                job

        Returns:
            job_types.JobInfo about the newly created pipeline job.
        """
        pass

    def get_status(self, job_id: job_types.JobId) -> job_types.JobDetails:
        """
        Get status of a job

        Args:
            job_id (job_types.JobId): job_id Unique identifier of the job to get the status of.

        Returns:
            job_types.JobDetails including the status of a known job
        """
        pass

    def list_jobs(self, job_filter: job_types.JobFilter = None) -> List[job_types.JobInfo]:
        """
        Provides list of currently running jobs

        Args:
            job_filter (job_types.JobFilter): Optional filter used to limit the number of
            pipeline job records return

        Returns:
            list of job_types.JobInfo with known pipeline job details from the server.
        """
        pass

    def start_job(self, job_id: job_types.JobId, named_values: Mapping[str, str] = None) -> job_types.JobToken:
        """
        Starts a "JobState.Pending" job.

        Once started, a job's payload becomes readonly.

        Args:
            job_id (job_types.JobId): Unique identifier of the job to start.
            named_values: Collection of name/value pairs used to populate pipeline
        variables.

        Returns:
            A job_types.JobToken with information on started job
        """
        pass

    def job_logs(self, job_id: job_types.JobId, operator_name: str) -> List[str]:
        """
        Retrieve logs of operator specified with "operator_name" with job associated with "job_id"

        Args:
            job_id (job_types.JobId): Unique identifier of the job to retrieve logs from
            operator_name (str): Operator to retrieve logs from

        Returns:
            List of operator logs
        """
        pass

    def add_metadata(self, job_id: job_types.JobId, metadata: Mapping[str, str]) -> Mapping[str, str]:
        """
        Requests the addition of metadata to a job.

        Args:
            job_id (job_types.JobId): Unique identifier of the job whose metadata is to be appended.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the job metadata. If a metadata
                    key in the request already exists in the job record, or if duplicate keys are passed in the request,
                    the job will not be updated and and an error will be returned. Keys are compared using case
                    insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes, while
                    the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the overall
                    metadata of an individual job is 4 Megabytes.

        Returns:
            A Mapping[str, str] containing the appended metadata
        """
        pass

    def remove_metadata(self, job_id: job_types.JobId, keys: List[str]) -> Mapping[str, str]:
        """
        Requests the removal of metadata from a job.

        Args:
            job_id: Unique identifier of the job whose metadata is to be removed.
            keys: List of keys to be removed from the job metadata.

        Returns:
            A Mapping[str, str] containing the updated set of metadata
        """
        pass


class JobsClient(BaseClient, JobsClientStub):

    def __init__(self, target: str, port: str = None, stub=None):
        """
        Jobs Client Creation

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
            self._stub = jobs_pb2_grpc.JobsStub(self._channel)
        else:
            self._stub = stub

    def close(self):
        """
        Close connection
        """
        if self._channel:
            self._channel.close()
            self._channel = None
            self._stub = None
        else:
            print("Connection for client already closed")

    def reconnect(self):
        """
        Re-open connection with existing channel
        """
        if self._channel is None:
            self._channel = grpc.insecure_channel(self._connection)
            self._stub = jobs_pb2_grpc.JobsStub(self._channel)
        else:
            print("Connection for client already open")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._channel is not None:
            self.close()
        return False

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

    def cancel_job(self, job_id: job_types.JobId, reason=None, timeout=None) -> job_types.JobToken:
        """
        Cancels a pipeline job, preventing it from being executed.

        Has no affect on executing or terminated jobs.

        Args:
            job_id (job_types.JobId): Unique identity of the job to be cancelled.
            reason: Optional reason as to why the job was cancelled.

        Returns:
            job_types.JobToken of cancelled job
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (job_id.value is None) or (job_id.value == ""):
            raise Exception("Job identifier must have instantiated value")

        request = jobs_pb2.JobsCancelRequest(header=self.get_request_header(), job_id=job_id.to_grpc_value(),
                                             reason=reason)

        response = self._stub.Cancel(request, timeout=timeout)

        self.check_response_header(header=response.header)

        result = job_types.JobToken(
            job_id=job_types.JobId(response.job_id.value),
            job_state=response.job_state,
            job_status=response.job_status
        )
        return result

    def create_job(self, pipeline_id: pipeline_types.PipelineId, job_name: str,
                   input_payloads: List[payload_types.PayloadId] = None,
                   job_priority: job_types.JobPriority = job_types.JobPriority.Normal,
                   metadata: Mapping[str, str] = None, timeout=None) -> job_types.JobInfo:
        """
        Creates a new pipeline job record and associate storage payload.

        Jobs are created in a "JobState.Pending" state.

        Use "StartJob(JobId, Map{KeyValuePair{string, string}}" to cause the job to start executing.

        Args:
            pipeline_id (pipeline_types.PipelineId): Unique identifier of the pipeline which the job should
                be instances from.
            job_name (str): Human readable name of the job.
            job_priority (job_types.JobPriority): Optional Priority of the job.
                Affects how and when the server will schedule the job.
            input_payloads (List[payload_types.PayloadId]): [Optional Paramater] List of static payloads to
                include as input for the job.
            metadata (Mapping[str, str]): [Optional Parameter] Metadata (set of key/value pairs) associated with the
                job

        Returns:
            job_types.JobInfo about the newly created pipeline job.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if pipeline_id.value is None:
            raise Exception("Pipeline identifier must have instantiated non-null instance")

        if (job_name is None) or (job_name == ""):
            raise Exception("Job name must be initialized to non-null/non-empty string")

        if (job_priority.value < job_types.JobPriority.Minimum.value) or (
                job_priority.value > job_types.JobPriority.Maximum.value):
            raise Exception("Job priority must contain valid value between minimum and maximum job priority bounds")

        input_payloads_identifiers = []

        if input_payloads is not None:
            for pay_id in input_payloads:
                input_payloads_identifiers.append(pay_id.to_grpc_value())
        else:
            input_payloads_identifiers = None

        request = jobs_pb2.JobsCreateRequest(
            header=self.get_request_header(),
            name=job_name,
            pipeline_id=pipeline_id.to_grpc_value(),
            priority=job_priority.value,
            input_payloads=input_payloads_identifiers
        )

        if metadata is not None:
            request.metadata.update(metadata)

        response = self._stub.Create(request, timeout=timeout)

        self.check_response_header(header=response.header)

        result = job_types.JobInfo(
            job_id=job_types.JobId(response.job_id.value),
            job_priority=job_priority,
            job_state=job_types.JobState.Pending,
            job_status=job_types.JobStatus.Healthy,
            name=job_name,
            payload_id=payload_types.PayloadId(value=response.payload_id.value),
            pipeline_id=pipeline_id,
            metadata=metadata
        )

        return result

    def get_status(self, job_id: job_types.JobId, timeout=None) -> job_types.JobDetails:
        """
        Get status of a job

        Args:
            job_id (job_types.JobId): job_id Unique identifier of the job to get the status of.

        Returns:
            job_types.JobDetails including the status of a known job
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if job_id.value is None:
            raise Exception("Job identifier must have instantiated non-null instance")

        request = jobs_pb2.JobsStatusRequest(header=self.get_request_header(), job_id=job_id.to_grpc_value())

        response = self._stub.Status(request, timeout=timeout)

        self.check_response_header(header=response.header)

        resp_operator_details = response.operator_details
        operator_details = {}

        for item in resp_operator_details:
            operator_details[item.name] = {}
            operator_details[item.name]["created"] = item.created
            operator_details[item.name]["started"] = item.started
            operator_details[item.name]["stopped"] = item.stopped
            operator_details[item.name]["status"] = item.status

        result = job_types.JobDetails(
            job_id=job_types.JobId(response.job_id.value),
            job_priority=response.priority,
            job_state=response.state,
            job_status=response.status,
            name=response.name,
            payload_id=payload_types.PayloadId(response.payload_id.value),
            pipeline_id=pipeline_types.PipelineId(response.pipeline_id.value),
            date_created=self.get_timestamp(response.created),
            date_started=self.get_timestamp(response.started),
            date_stopped=self.get_timestamp(response.stopped),
            operator_details=operator_details,
            messages=response.messages,
            metadata=response.metadata
        )

        return result

    def list_jobs(self, job_filter: job_types.JobFilter = None, timeout=None) -> List[job_types.JobInfo]:
        """
        Provides list of currently running jobs

        Args:
            job_filter (job_types.JobFilter): Optional filter used to limit the number of
            pipeline job records return

        Returns:
            list of job_types.JobInfo with known pipeline job details from the server.
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        empty = job_types.JobFilter()

        request = jobs_pb2.JobsListRequest(
            header=self.get_request_header()
        )

        if job_filter != empty and job_filter is not None:
            request_filter = jobs_pb2.JobsListRequest.JobFilter

            if job_filter.completed_before is not None:
                seconds = (job_filter.completed_before - datetime.datetime(1, 1, 1)).total_seconds()
                request.filter.completed_before.value = int(seconds)

            if job_filter.created_after is not None:
                seconds = (job_filter.created_after - datetime.datetime(1, 1, 1)).total_seconds()
                request.filter.created_after.value = int(seconds)

            if job_filter.has_job_state is not None:
                if len(job_filter.has_job_state) > 0:
                    for state in job_filter.has_job_state:
                        if (state.value < job_types.JobState.Minimum.value) or (
                                state.value > job_types.JobState.Maximum.value):
                            raise Exception("Job states in filter must be within " + str(
                                job_types.JobState.Minimum) + " and " + str(
                                job_types.JobState.Maximum) + ", found:" + str(state))

                        request.filter.has_state.append(state.value)

            if job_filter.has_job_status is not None:
                if len(job_filter.has_job_status) > 0:
                    for status in job_filter.has_job_status:
                        if (status.value < job_types.JobStatus.Minimum.value) or (
                                status.value > job_types.JobStatus.Maximum.value):
                            raise Exception("Job status in filter must be within " + str(
                                job_types.JobStatus.Minimum) + " and " + str(
                                job_types.JobStatus.Maximum) + ", found:" + str(status))

                        request.filter.has_status.append(status.value)

            if job_filter.pipeline_ids is not None:
                if len(job_filter.pipeline_ids) > 0:
                    for pipe_id in job_filter.pipeline_ids:
                        request.filter.pipeline_id.append(pipe_id.to_grpc_value())

        response = self._stub.List(request, timeout=timeout)

        responses = [resp for resp in response]

        result = []

        if len(responses) > 0:

            self.check_response_header(header=responses[0].header)

            for item in responses:

                if (item.job_details is None) or (item.job_details.job_id.value == ''):
                    continue

                info = job_types.JobInfo(
                    job_id=job_types.JobId(item.job_details.job_id.value),
                    job_priority=item.job_details.priority,
                    job_state=item.job_details.state,
                    job_status=item.job_details.status,
                    name=item.job_details.job_name,
                    payload_id=payload_types.PayloadId(item.job_details.payload_id.value),
                    pipeline_id=pipeline_types.PipelineId(item.job_details.pipeline_id.value),
                    date_created=self.get_timestamp(item.job_details.created),
                    date_started=self.get_timestamp(item.job_details.started),
                    date_stopped=self.get_timestamp(item.job_details.stopped),
                    metadata=item.job_details.metadata
                )

                result.append(info)

        return result

    def start_job(self, job_id: job_types.JobId, named_values: Mapping[str, str] = None,
                  timeout=None) -> job_types.JobToken:
        """
        Starts a "JobState.Pending" job.

        Once started, a job's payload becomes readonly.

        Args:
            job_id (job_types.JobId): Unique identifier of the job to start.
            named_values: Collection of name/value pairs used to populate pipeline
        variables.

        Returns:
            A job_types.JobToken with information on started job
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (job_id.value is None) or (job_id.value == ""):
            raise Exception("Job identifier must have instantiated value")

        request = jobs_pb2.JobsStartRequest(
            header=self.get_request_header(),
            job_id=job_id.to_grpc_value()
        )

        if named_values is not None:
            for item in named_values.keys():
                nvp = jobs_pb2.JobsStartRequest.NamedValue(
                    name=item,
                    value=named_values.get(item)
                )

                request.Variables.append(nvp)

        response = self._stub.Start(request, timeout=timeout)

        self.check_response_header(header=response.header)

        result = job_types.JobToken(
            job_id=job_id,
            job_priority=response.priority,
            job_state=response.state,
            job_status=response.status
        )

        return result

    def job_logs(self, job_id: job_types.JobId, operator_name: str, timeout=None) -> List[str]:
        """
        Retrieve logs of operator specified with "operator_name" with job associated with "job_id"

        Args:
            job_id (job_types.JobId): Unique identifier of the job to retrieve logs from
            operator_name (str): Operator to retrieve logs from

        Returns:
            List of operator logs
        """

        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (job_id.value is None) or (job_id.value == ""):
            raise Exception("Job identifier must have instantiated value")

        if operator_name is None:
            raise Exception("Operator must have valid instantiated value")

        if operator_name.strip() == "":
            raise Exception("Operator must have valid instantiated value")

        request = jobs_pb2.JobsReadLogsRequest(
            header=self.get_request_header(),
            job_id=job_id.to_grpc_value(),
            operator_name=operator_name
        )

        response = self._stub.ReadLogs(request, timeout=timeout)

        responses = [resp.logs for resp in response]

        logs_list = []
        for resp in responses:
            for log in resp:
                logs_list.append(log)

        return logs_list

    def add_metadata(self, job_id: job_types.JobId, metadata: Mapping[str, str], timeout=None) -> Mapping[str, str]:
        """
        Requests the addition of metadata to a job.

        Args:
            job_id (job_types.JobId): Unique identifier of the job whose metadata is to be appended.
            metadata(Mapping[str, str]): Set of key/value pairs to be appended to the job metadata. If a metadata
                    key in the request already exists in the job record, or if duplicate keys are passed in the request,
                    the job will not be updated and and an error will be returned. Keys are compared using case
                    insensitive comparator functions. The maximum allowed size of a metadata key is 128 bytes, while
                    the maximum allowed size of a metadata value is 256 bytes. The maximum allowed size for the overall
                    metadata of an individual job is 4 Megabytes.

        Returns:
            A Mapping[str, str] object containing the appended metadata

        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (job_id.value is None) or (job_id.value == ""):
            raise Exception("Job identifier must have instantiated value")

        if metadata is None:
            raise Exception("Metadata must be an instantiated map")

        request = jobs_pb2.JobsAddMetadataRequest(
            job_id=job_id.to_grpc_value()
        )

        request.metadata.update(metadata)

        response = self._stub.AddMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result

    def remove_metadata(self, job_id: job_types.JobId, keys: List[str], timeout=None) -> Mapping[str, str]:
        """
        Requests the removal of metadata from a job.

        Args:
            job_id: Unique identifier of the job whose metadata is to be removed.
            keys: List of keys to be removed from the job metadata.

        Returns:
            A Mapping[str, str] object containing the updated set of metadata
        """
        if (self._channel is None) or (self._stub is None):
            raise Exception("Connection is currently closed. Please run reconnect() to reopen connection")

        if (job_id.value is None) or (job_id.value == ""):
            raise Exception("Job identifier must have instantiated value")

        if keys is None:
            raise Exception("Keys paramater must be valid list of metadata keys")

        request = jobs_pb2.JobsRemoveMetadataRequest(
            job_id=job_id.to_grpc_value()
        )

        request.keys.extend(keys)

        response = self._stub.RemoveMetadata(request, timeout)

        self.check_response_header(header=response.header)

        result = response.metadata

        return result
