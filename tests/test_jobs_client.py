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


import nvidia_clara.grpc.common_pb2 as common_pb2
import nvidia_clara.grpc.jobs_pb2 as jobs_pb2

from nvidia_clara.base_client import BaseClient
from nvidia_clara.jobs_client import JobsClient
import nvidia_clara.pipeline_types as pipeline_types
import nvidia_clara.job_types as job_types

from tests.test_client_tools import run_client_test


def run_job_client(stub, method_name, *args, **kwargs):
    with JobsClient(target='10.0.0.1:50051', stub=stub) as client:
        response = getattr(client, method_name)(*args, **kwargs)
        return response


class MockClaraJobsServiceClient:
    stub_method_handlers = []

    def __init__(self, channel, stub=None, request_header=None, logger=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def create_job(self, *args, **kwargs):
        return run_client_test(
            'Jobs',
            'create_job',
            run_job_client,
            stub_method_handlers=MockClaraJobsServiceClient.stub_method_handlers,
            *args, **kwargs)

    def cancel_job(self, *args, **kwargs):
        return run_client_test(
            'Jobs',
            'cancel_job',
            run_job_client,
            stub_method_handlers=MockClaraJobsServiceClient.stub_method_handlers,
            *args, **kwargs)

    def get_status(self, *args, **kwargs):
        return run_client_test(
            'Jobs',
            'get_status',
            run_job_client,
            stub_method_handlers=MockClaraJobsServiceClient.stub_method_handlers,
            *args, **kwargs)

    def list_jobs(self, *args, **kwargs):
        return run_client_test(
            'Jobs',
            'list_jobs',
            run_job_client,
            stub_method_handlers=MockClaraJobsServiceClient.stub_method_handlers,
            *args, **kwargs)

    def start_job(self, *args, **kwargs):
        return run_client_test(
            'Jobs',
            'start_job',
            run_job_client,
            stub_method_handlers=MockClaraJobsServiceClient.stub_method_handlers,
            *args, **kwargs)

    def job_logs(self, *args, **kwargs):
        return run_client_test(
            'Jobs',
            'job_logs',
            run_job_client,
            stub_method_handlers=MockClaraJobsServiceClient.stub_method_handlers,
            *args, **kwargs)

    def close(self):
        pass


def test_create_job():
    requests = [
        jobs_pb2.JobsCreateRequest(
            header=BaseClient.get_request_header(),
            name='test job',
            pipeline_id=common_pb2.Identifier(
                value='92656d79fa414db6b294069c0e9e6df5'
            ),
            priority=jobs_pb2.JOB_PRIORITY_NORMAL
        )
    ]

    responses = [
        jobs_pb2.JobsCreateResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            ),
            payload_id=common_pb2.Identifier(
                value='7ac5c691e13d4f45894a3a70d9925936'
            )
        )
    ]

    stub_method_handlers = [(
        'Create',
        'unary_unary',
        (
            requests,
            responses
        )
    )]

    MockClaraJobsServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraJobsServiceClient('localhost:50051') as client:
        job_info = client.create_job(
            job_name='test job',
            pipeline_id=pipeline_types.PipelineId('92656d79fa414db6b294069c0e9e6df5')
        )

        print(job_info.job_id.value, job_info.payload_id.value)

        assert job_info.job_id.value == '432b274a8f754968888807fe1eba237b'
        assert job_info.payload_id.value == '7ac5c691e13d4f45894a3a70d9925936'


def test_cancel_job():
    requests = [
        jobs_pb2.JobsCancelRequest(
            header=BaseClient.get_request_header(),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            )
        )
    ]

    responses = [
        jobs_pb2.JobsCancelResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            ),
            job_state=jobs_pb2.JOB_STATE_STOPPED,
            job_status=jobs_pb2.JOB_STATUS_CANCELED
        )
    ]

    stub_method_handlers = [(
        'Cancel',
        'unary_unary',
        (
            requests,
            responses
        )
    )]

    MockClaraJobsServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraJobsServiceClient('10.0.0.1:50051') as client:
        job_token = client.cancel_job(
            job_id=job_types.JobId(value='432b274a8f754968888807fe1eba237b')
        )

        print(job_token.job_id.value, job_token.job_state, job_token.job_status)

        assert job_token.job_id.value == '432b274a8f754968888807fe1eba237b'
        assert job_token.job_state == 3
        assert job_token.job_status == 3


def test_get_status():
    requests = [
        jobs_pb2.JobsStatusRequest(
            header=BaseClient.get_request_header(),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            )
        )
    ]

    fake_seconds_from_epoch = 63763345820

    responses = [
        jobs_pb2.JobsStatusResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            name="job_1",
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            ),
            pipeline_id=common_pb2.Identifier(
                value='92656d79fa414db6b294069c0e9e6df5'
            ),
            payload_id=common_pb2.Identifier(
                value='7ac5c691e13d4f45894a3a70d9925936'
            ),
            state=jobs_pb2.JOB_STATE_RUNNING,
            status=jobs_pb2.JOB_STATUS_HEALTHY,
            created=common_pb2.Timestamp(value=fake_seconds_from_epoch)
        )
    ]

    stub_method_handlers = [(
        'Status',
        'unary_unary',
        (
            requests,
            responses
        )
    )]

    MockClaraJobsServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraJobsServiceClient('10.0.0.1:50051') as client:
        job_details = client.get_status(
            job_id=job_types.JobId(value='432b274a8f754968888807fe1eba237b')
        )

        print(job_details.job_id.value, job_details.job_state, job_details.job_status)
        print(job_details.date_created)
        print(datetime.datetime.fromtimestamp(float(fake_seconds_from_epoch) - 62135596800))

        assert job_details.name == "job_1"
        assert job_details.job_id.value == '432b274a8f754968888807fe1eba237b'
        assert job_details.pipeline_id.value == '92656d79fa414db6b294069c0e9e6df5'
        assert job_details.payload_id.value == '7ac5c691e13d4f45894a3a70d9925936'
        assert job_details.job_state == 2
        assert job_details.job_status == 1
        assert job_details.date_created == datetime.datetime.fromtimestamp(
            float(fake_seconds_from_epoch) - 62135596800).astimezone(datetime.timezone.utc)


def test_list_jobs():
    requests = [
        jobs_pb2.JobsListRequest(
            header=BaseClient.get_request_header()
        )
    ]

    responses = [
        jobs_pb2.JobsListResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            job_details=jobs_pb2.JobsListResponse.JobDetails(
                job_name="job_1",
                job_id=common_pb2.Identifier(
                    value="432b274a8f754968888807fe1eba237b"
                ),
                payload_id=common_pb2.Identifier(
                    value='532b274a8f754968888807fe1eba237b'
                ),
                pipeline_id=common_pb2.Identifier(
                    value='932b274a8f754968888807fe1eba237b'
                ),
                created=common_pb2.Timestamp(
                    value=63750823591

                )
            )
        ),
        jobs_pb2.JobsListResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            job_details=jobs_pb2.JobsListResponse.JobDetails(
                job_name="job_2",
                job_id=common_pb2.Identifier(
                    value='212b274a8f754968888807fe1eba237b'
                ),
                payload_id=common_pb2.Identifier(
                    value='212b274a8f754968888807fe1eba237b'
                ),
                pipeline_id=common_pb2.Identifier(
                    value='322b274a8f754968888807fe1eba237b'
                ),
                created=common_pb2.Timestamp(
                    value=63750823591

                )
            )
        )
    ]

    stub_method_handlers = [(
        'List',
        'unary_stream',
        (
            requests,
            responses
        )
    )]

    MockClaraJobsServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraJobsServiceClient('10.0.0.1:50051') as client:
        list_jobs = client.list_jobs()

        print("Length of list response: " + str(len(list_jobs)))

        assert len(list_jobs) == 2

        assert list_jobs[0].name == "job_1"
        assert list_jobs[0].job_id.value == "432b274a8f754968888807fe1eba237b"
        assert list_jobs[0].payload_id.value == "532b274a8f754968888807fe1eba237b"
        assert list_jobs[0].pipeline_id.value == "932b274a8f754968888807fe1eba237b"
        assert list_jobs[0].date_created == datetime.datetime(2021, 3, 8, 18, 6, 31, tzinfo=datetime.timezone.utc)

        assert list_jobs[1].name == "job_2"
        assert list_jobs[1].job_id.value == '212b274a8f754968888807fe1eba237b'
        assert list_jobs[1].payload_id.value == '212b274a8f754968888807fe1eba237b'
        assert list_jobs[1].pipeline_id.value == '322b274a8f754968888807fe1eba237b'
        assert list_jobs[1].date_created == datetime.datetime(2021, 3, 8, 18, 6, 31, tzinfo=datetime.timezone.utc)


def test_start_job():
    requests = [
        jobs_pb2.JobsStartRequest(
            header=BaseClient.get_request_header(),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            )
        )
    ]

    responses = [
        jobs_pb2.JobsStartResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            state=jobs_pb2.JOB_STATE_RUNNING,
            status=jobs_pb2.JOB_STATUS_HEALTHY,
            priority=jobs_pb2.JOB_PRIORITY_NORMAL
        )
    ]

    stub_method_handlers = [(
        'Start',
        'unary_unary',
        (
            requests,
            responses
        )
    )]

    MockClaraJobsServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraJobsServiceClient('10.0.0.1:50051') as client:
        job_token = client.start_job(
            job_id=job_types.JobId(value='432b274a8f754968888807fe1eba237b')
        )

        print(job_token.job_id.value, job_token.job_state, job_token.job_status)

        assert job_token.job_id.value == '432b274a8f754968888807fe1eba237b'
        assert job_token.job_state == 2
        assert job_token.job_status == 1


def test_read_logs():
    requests = [
        jobs_pb2.JobsReadLogsRequest(
            header=BaseClient.get_request_header(),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            ),
            operator_name="dicom-reader"
        )
    ]

    responses = [
        jobs_pb2.JobsReadLogsResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            ),
            operator_name="Dicom Reader",
            logs=["Log_String_0", "Log_String_1"]
        ),
        jobs_pb2.JobsReadLogsResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            job_id=common_pb2.Identifier(
                value='432b274a8f754968888807fe1eba237b'
            ),
            operator_name="Dicom Reader",
            logs=["Log_String_2", "Log_String_3"]
        )
    ]

    stub_method_handlers = [(
        'ReadLogs',
        'unary_stream',
        (
            requests,
            responses
        )
    )]

    MockClaraJobsServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraJobsServiceClient('10.0.0.1:50051') as client:
        job_logs = client.job_logs(
            job_id=job_types.JobId(value='432b274a8f754968888807fe1eba237b'),
            operator_name="dicom-reader"
        )

        print(len(job_logs))

        assert len(job_logs) == 4
        assert job_logs[0] == "Log_String_0"
        assert job_logs[1] == "Log_String_1"
        assert job_logs[2] == "Log_String_2"
        assert job_logs[3] == "Log_String_3"
