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
from enum import Enum
from typing import List, Mapping, TypeVar
from nvidia_clara.grpc import common_pb2, jobs_pb2
import nvidia_clara.payload_types as payload_types
import nvidia_clara.pipeline_types as pipeline_types

T = TypeVar('T')


class JobPriority(Enum):
    """
    Priority of a pipeline job.
    A job's priority affects the order it will be scheduled once "JobsClient.StartJob()" is called.
    """

    # The job priority is unknown.
    Unkown = 0

    # Lower than normal priority.
    # Lower priority jobs are queued like "Normal" and "Higher" priority jobs, but are scheduled less frequently.
    Lower = jobs_pb2.JOB_PRIORITY_LOWER

    # Normal, or default, priority.
    # Normal priority jobs are queued like "Lower" and "Higher" priority jobs.
    # Normal priority jobs are scheduled more frequently than lower priority jobs and less frequently then higher priority jobs.
    Normal = jobs_pb2.JOB_PRIORITY_NORMAL

    # Higher than normal priority.
    # Higher priority jobs are queued like "Lower" and "Normal"priority jobs, but are scheduled more frequently.
    Higher = jobs_pb2.JOB_PRIORITY_HIGHER

    # Immediate priority jobs are queued in separate queue which must emptied before any other priority jobs can be scheduled.
    Immediate = jobs_pb2.JOB_PRIORITY_IMMEDIATE

    # The maximum supported value for "JobPriority"
    Maximum = jobs_pb2.JOB_PRIORITY_IMMEDIATE

    # The minimum supported value for "JobPriority"
    Minimum = jobs_pb2.JOB_PRIORITY_LOWER


class JobState(Enum):
    """
    State of a pipeline job.
    Related to "JobStatus"
    """

    # The job is unknown or in an unknown state.
    Unknown = 0

    # The job has been accepted and queued by the server, but has not yet started running.
    Pending = jobs_pb2.JOB_STATE_PENDING

    # The job is currently running.
    Running = jobs_pb2.JOB_STATE_RUNNING

    # The job has stopped runing.
    Stopped = jobs_pb2.JOB_STATE_STOPPED

    # Maximum supported value of "JobState"
    Maximum = Stopped

    # Minimum supported value of "JobState"
    Minimum = Pending


class JobStatus(Enum):
    """
    Status of  pipeline job.
    Related to "JobState"
    """
    # The job is unknown or the status of the job is unknown.
    Unknown = 0

    # The job has been canceled.
    Canceled = jobs_pb2.JOB_STATUS_CANCELED

    # The job has encountered a terminal error.
    Faulted = jobs_pb2.JOB_STATUS_FAULTED

    # The job is healthy.
    # If stopped, the job has completed successfully.
    Healthy = jobs_pb2.JOB_STATUS_HEALTHY

    # The job was evicted
    Evicted = jobs_pb2.JOB_STATUS_EVICTED

    # The job was terminated
    Terminated = jobs_pb2.JOB_STATUS_TERMINATED

    # Maximum supported value of "JobStatus"
    Maximum = Canceled

    # Minimum supported value of "JobStatus"
    Minimum = Healthy


class JobOperatorStatus(Enum):
    """
    Status of an operator in a job.
    """
    # The operator is unknownor the status of the operator is unknown.
    Unknown = jobs_pb2.JOB_OPERATOR_STATUS_UNKNOWN

    # The operator has been accepted and queued by the server, but has not yet started running.
    Pending = jobs_pb2.JOB_OPERATOR_STATUS_PENDING

    # The operator is currently running.
    Running = jobs_pb2.JOB_OPERATOR_STATUS_RUNNING

    # The operator has completed successfully.
    Completed = jobs_pb2.JOB_OPERATOR_STATUS_COMPLETED

    # The operator has encountered an error.
    Faulted = jobs_pb2.JOB_OPERATOR_STATUS_FAULTED


class JobId:
    """
    Unique identifier for a Clara Pipeline Job.
    """

    def __init__(self, value: str):
        """
        Creates Unique Identifier Object for a Clara Pipeline Job.
        """
        if (value is None) or (value == ""):
            raise Exception("Job identifier value must be intialized.")

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


class JobToken:

    def __init__(self, job_id: JobId = None, job_state: JobState = None, job_status: JobStatus = None,
                 job_priority: JobPriority = None):
        self._job_id = job_id
        self._job_state = job_state
        self._job_status = job_status
        self._job_priority = job_priority

    @property
    def job_id(self) -> JobId:
        """Unique identifier of the job."""
        return self._job_id

    @job_id.setter
    def job_id(self, job_id: JobId):
        """Unique identifier of the job."""
        self._job_id = job_id

    @property
    def job_state(self) -> JobState:
        """Current state of the job."""
        return self._job_state

    @job_state.setter
    def job_state(self, job_state: JobState):
        """Current state of the job."""
        self._job_state = job_state

    @property
    def job_status(self) -> JobStatus:
        """Current status of the job."""
        return self._job_status

    @job_status.setter
    def job_status(self, job_status: JobStatus):
        """Current status of the job."""
        self._job_status = job_status

    @property
    def job_priority(self) -> JobPriority:
        """Priority of the job"""
        return self._job_priority

    @job_priority.setter
    def job_priority(self, job_priority: JobPriority):
        """Priority of the job"""
        self._job_priority = job_priority


class JobInfo(JobToken):

    def __init__(self, job_id: JobId = None, job_state: JobState = None, job_status: JobStatus = None,
                 job_priority: JobPriority = None, date_created: datetime = None, date_started: datetime = None,
                 date_stopped: datetime = None, name: str = None, payload_id: payload_types.PayloadId = None,
                 pipeline_id: pipeline_types.PipelineId = None, metadata: Mapping[str, str] = None):
        super().__init__(
            job_id=job_id,
            job_state=job_state,
            job_status=job_status,
            job_priority=job_priority,
        )
        if metadata is None:
            metadata = dict()

        self._date_created = date_created
        self._date_started = date_started
        self._date_stopped = date_stopped
        self._name = name
        self._payload_id = payload_id
        self._pipeline_id = pipeline_id
        self._metadata = metadata

    @property
    def date_created(self) -> datetime:
        return self._date_created

    @date_created.setter
    def date_created(self, date_created: datetime):
        self._date_created = date_created

    @property
    def date_started(self) -> datetime:
        return self._date_started

    @date_started.setter
    def date_started(self, date_started: datetime):
        self._date_started = date_started

    @property
    def date_stopped(self) -> datetime:
        return self._date_stopped

    @date_stopped.setter
    def date_stopped(self, date_stopped: datetime):
        self._date_stopped = date_stopped

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def payload_id(self) -> payload_types.PayloadId:
        return self._payload_id

    @payload_id.setter
    def payload_id(self, payload_id: payload_types.PayloadId):
        self._payload_id = payload_id

    @property
    def pipeline_id(self) -> pipeline_types.PipelineId:
        return self._pipeline_id

    @pipeline_id.setter
    def pipeline_id(self, pipeline_id: pipeline_types.PipelineId):
        self._pipeline_id = pipeline_id

    @property
    def metadata(self) -> Mapping[str, str]:
        """
        Metadata (set of key/value pairs) associated with the job
        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Mapping[str, str]):
        """
        Metadata (set of key/value pairs) associated with the job
        """
        self._metadata = metadata


class JobFilter:

    def __init__(self, completed_before: datetime = None, created_after: datetime = None,
                 has_job_state: List[JobState] = None, has_job_status: List[JobStatus] = None,
                 pipeline_ids: List[pipeline_types.PipelineId] = None):
        if has_job_state is None:
            has_job_state = []
        if has_job_status is None:
            has_job_status = []
        if pipeline_ids is None:
            pipeline_ids = []

        self._completed_before = completed_before
        self._created_after = created_after
        self._has_job_state = has_job_state
        self._has_job_status = has_job_status
        self._pipeline_ids = pipeline_ids

    @property
    def completed_before(self) -> datetime:
        """When applied, only jobs completed before the supplied date will be returned."""
        return self._completed_before

    @completed_before.setter
    def completed_before(self, completed_before: datetime):
        """When applied, only jobs completed before the supplied date will be returned."""
        self._completed_before = completed_before

    @property
    def created_after(self) -> datetime:
        """When applied, only jobs created after the supplied date will be returned."""
        return self._created_after

    @created_after.setter
    def created_after(self, created_after: datetime):
        """When applied, only jobs created after the supplied date will be returned."""
        self._created_after = created_after

    @property
    def has_job_state(self) -> List[JobState]:
        """When applied, only jobs having a provided state value will be returned."""
        return self._has_job_state

    @has_job_state.setter
    def has_job_state(self, has_job_state: List[JobState]):
        """When applied, only jobs having a provided state value will be returned."""
        self._has_job_state = has_job_state

    @property
    def has_job_status(self) -> List[JobStatus]:
        """When applied, only jobs having a provided status value will be returned."""
        return self._has_job_status

    @has_job_status.setter
    def has_job_status(self, has_job_status: List[JobStatus]):
        """When applied, only jobs having a provided status value will be returned."""
        self._has_job_status = has_job_status

    @property
    def pipeline_ids(self) -> List[pipeline_types.PipelineId]:
        """When applied, only jobs with matching pipeline identifiers will be returned."""
        return self._pipeline_ids

    @pipeline_ids.setter
    def pipeline_ids(self, pipeline_ids: List[pipeline_types.PipelineId]):
        """When applied, only jobs with matching pipeline identifiers will be returned."""
        self._pipeline_ids = pipeline_ids


class JobDetails(JobInfo):

    def __init__(self, job_id: JobId = None, job_state: JobState = None, job_status: JobStatus = None,
                 job_priority: JobPriority = None, date_created: datetime = None, date_started: datetime = None,
                 date_stopped: datetime = None, name: str = None, payload_id: payload_types.PayloadId = None,
                 pipeline_id: pipeline_types.PipelineId = None, operator_details: Mapping[str, Mapping[str, T]] = None,
                 messages: List[str] = None, metadata: Mapping[str, str] = None):
        if metadata is None:
            metadata = dict()

        super().__init__(
            job_id=job_id,
            job_state=job_state,
            job_status=job_status,
            job_priority=job_priority,
            date_created=date_created,
            date_started=date_started,
            date_stopped=date_stopped,
            name=name,
            payload_id=payload_id,
            pipeline_id=pipeline_id,
            metadata=metadata
        )

        if messages is None:
            messages = []
        if operator_details is None:
            operator_details = dict()

        self._messages = messages
        self._operator_details = operator_details

    @property
    def messages(self) -> List[str]:
        """List of messages reported by the job."""
        return self._messages

    @messages.setter
    def messages(self, messages: List[str]):
        """List of messages reported by the job."""
        self._messages = messages

    @property
    def operator_details(self) -> Mapping[str, Mapping[str, T]]:
        """Dictionary mapping operator names to operator details"""
        return self._operator_details

    @operator_details.setter
    def operator_details(self, operator_details: Mapping[str, Mapping[str, T]]):
        """Dictionary mapping operator names to operator details"""
        self._operator_details = operator_details
