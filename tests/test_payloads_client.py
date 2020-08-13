# Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
#
# NVIDIA CORPORATION and its licensors retain all intellectual property
# and proprietary rights in and to this software, related documentation
# and any modifications thereto.  Any use, reproduction, disclosure or
# distribution of this software and related documentation without an express
# license agreement from NVIDIA CORPORATION is strictly prohibited.

import os

import nvidia_clara.grpc.common_pb2 as common_pb2
import nvidia_clara.grpc.payloads_pb2 as payloads_pb2

from nvidia_clara.base_client import BaseClient
from nvidia_clara.payloads_client import PayloadsClient
import nvidia_clara.payload_types as payload_types

from tests.test_jobs_client import run_client_test


def run_payload_client(stub, method_name, *args, **kwargs):
    with PayloadsClient(target='10.0.0.1:50051', stub=stub) as client:
        response = getattr(client, method_name)(*args, **kwargs)
        return response


class MockClaraPayloadServiceClient:
    stub_method_handlers = []

    def __init__(self, channel, stub=None, request_header=None, logger=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def create_payload(self, *args, **kwargs):
        return run_client_test(
            'Payloads',
            'create_payload',
            run_payload_client,
            stub_method_handlers=MockClaraPayloadServiceClient.stub_method_handlers,
            *args, **kwargs)

    def download_from(self, *args, **kwargs):
        return run_client_test(
            'Payloads',
            'download_from',
            run_payload_client,
            stub_method_handlers=MockClaraPayloadServiceClient.stub_method_handlers,
            *args, **kwargs)

    def upload(self, *args, **kwargs):
        return run_client_test(
            'Payloads',
            'upload',
            run_payload_client,
            stub_method_handlers=MockClaraPayloadServiceClient.stub_method_handlers,
            *args, **kwargs)

    def close(self):
        pass


def test_create_payload():
    requests = [
        payloads_pb2.PayloadsCreateRequest(
            header=BaseClient.get_request_header()
        )
    ]

    responses = [
        payloads_pb2.PayloadsCreateResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            payload_id=common_pb2.Identifier(
                value='92656d79fa414db6b294069c0e9e6df5'
            ),
            type=payloads_pb2.PAYLOAD_TYPE_REUSABLE
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

    # set handlers
    MockClaraPayloadServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraPayloadServiceClient('localhost:50051') as client:
        payload_details = client.create_payload()
        print(payload_details.payload_id)
        print(payload_details.payload_type)
        assert payload_details.payload_id.value == '92656d79fa414db6b294069c0e9e6df5'
        assert payload_details.payload_type == 2


MHD_TEXT = '''ObjectType = Image
NDims = 3
BinaryData = True
BinaryDataByteOrderMSB = False
CompressedData = False
TransformMatrix = -1 0 0 0 1 0 0 0 1
Offset = 0 0 0
CenterOfRotation = 0 0 0
AnatomicalOrientation = RAI
ElementSpacing = 0.98 0.98 1.5
DimSize = 460 286 1182
ElementType = MET_SHORT
ElementDataFile = highResCT.raw
'''


def test_download_file():
    fake_payload_id = '7ac5c691e13d4f45894a3a70d9925936'
    fake_request_file_name = '/input/highResCT.mhd'

    requests = [
        payloads_pb2.PayloadsDownloadRequest(
            header=BaseClient.get_request_header(),
            payload_id=common_pb2.Identifier(value=fake_payload_id),
            name=fake_request_file_name)
    ]

    responses = [
        payloads_pb2.PayloadsDownloadResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            details=payloads_pb2.PayloadFileDetails(mode=0, name=fake_request_file_name,
                                                    size=len(MHD_TEXT)),
            data=MHD_TEXT.encode('utf-8')
        )
    ]

    stub_method_handlers = [(
        'Download',
        'unary_stream',
        (
            requests,
            responses
        )
    )]

    MockClaraPayloadServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraPayloadServiceClient('localhost:50051') as client:
        if os.path.exists('./highResCT.mhd'):
            os.remove('./highResCT.mhd')

        with open('./highResCT.mhd', 'wb+') as wb:
            file_details = client.download_from(payload_id=payload_types.PayloadId(fake_payload_id),
                                                blob_name=fake_request_file_name,
                                                dest_obj=wb)
            assert file_details.mode == 0
            assert file_details.name == fake_request_file_name
            assert file_details.size == len(MHD_TEXT)

        data = ''

        with open('./highResCT.mhd', 'r') as file:
            data = file.read()

        os.remove('./highResCT.mhd')

        print("Data Returned: ")
        print(data)

        assert data == MHD_TEXT


def test_upload(tmp_path):
    fake_payload_id = '7ac5c691e13d4f45894a3a70d9925936'
    fake_file_name = './image.mhd'
    fake_response_file_name = './input/image.mhd'

    requests = [
        payloads_pb2.PayloadsUploadRequest(
            header=BaseClient.get_request_header(),
            payload_id=common_pb2.Identifier(value=fake_payload_id),
            details=payloads_pb2.PayloadFileDetails(mode=0, name=fake_response_file_name, size=len(MHD_TEXT)),
            data=MHD_TEXT.encode('utf-8')
        )
    ]
    responses = [
        payloads_pb2.PayloadsUploadResponse(
            header=common_pb2.ResponseHeader(
                code=0,
                messages=[]),
            details=payloads_pb2.PayloadFileDetails(mode=0, name=fake_response_file_name,
                                                    size=len(MHD_TEXT))
        )
    ]

    stub_method_handlers = [(
        'Upload',
        'stream_unary',
        (
            requests,
            responses
        )
    )]

    MockClaraPayloadServiceClient.stub_method_handlers = stub_method_handlers

    with MockClaraPayloadServiceClient('localhost:50051') as client:
        if os.path.exists(fake_file_name):
            os.remove(fake_file_name)

        with open(fake_file_name, 'w') as wb:
            wb.write(MHD_TEXT)

        file_details = None

        with open(fake_file_name, 'rb+') as fp:
            file_details = client.upload(payload_id=payload_types.PayloadId(fake_payload_id),
                                         blob_name=fake_response_file_name, file_object=fp)

        os.remove(fake_file_name)

        print(file_details.mode, file_details.name, file_details.size)

        assert file_details.mode == 0
        assert file_details.name == fake_response_file_name
        assert file_details.size == len(MHD_TEXT)
