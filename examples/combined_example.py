# Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
#
# NVIDIA CORPORATION and its licensors retain all intellectual property
# and proprietary rights in and to this software, related documentation
# and any modifications thereto.  Any use, reproduction, disclosure or
# distribution of this software and related documentation without an express
# license agreement from NVIDIA CORPORATION is strictly prohibited.

from nvidia_clara.jobs_client import JobsClient
from nvidia_clara.pipelines_client import PipelinesClient
from nvidia_clara.payloads_client import PayloadsClient
import nvidia_clara.pipeline_types as pipeline_types
import os
from pathlib import Path

# Clients creation
clara_ip_address = "10.0.0.1"
clara_port = "31851"

jobs_client = JobsClient(target=clara_ip_address, port=clara_port)
payloads_client = PayloadsClient(target=clara_ip_address, port=clara_port)
pipeline_client = PipelinesClient(target=clara_ip_address, port=clara_port)

# Create list of pipeline_types.PipelineDefinition with local path to pipeline .yaml
file_path = "../spleen_pipeline.yaml"
definitions = [pipeline_types.PipelineDefinition(name=file_path, content=Path(file_path).read_text())]

# Create Pipeline with definition list created
pipeline_id = pipeline_client.create_pipeline(definition=definitions)

# Create Job with newly created Pipeline
job_info = jobs_client.create_job(job_name="spleenjob", pipeline_id=pipeline_types.PipelineId(pipeline_id.value))
job_id = job_info.job_id
payload_id = job_info.payload_id

# Local path to directory of files to upload to the job's payload on the Server
input_path = "../app_spleen-input_v1/dcm"

# Go through files in directory and upload to the job using the payload identifier
for file in os.listdir(input_path):
    file_path = input_path + "/" + str(file)
    with open(file_path, 'rb') as fp:
        payloads_client.upload(payload_id=payload_id, blob_name=file, file_object=fp)

# Get a List of the jobs
job_list = jobs_client.list_jobs()

# Start Job
job_token = jobs_client.start_job(job_id=job_id)

# Loop until job completes
job_status = jobs_client.get_status(job_id=job_id)
while job_status.job_state != 3:
    job_status = jobs_client.get_status(job_id=job_id)

# Get Payload Details - Used to get list of payload files
payload_details = payloads_client.get_details(payload_id=payload_id)

# Download files from payload if pertaining to output payload directory (ex. "/operators)
for file in payload_details.file_details:

    # Get file path on Server (ex. /operators/dicom-reader/example_file.raw")
    file_name = file.name

    # Split file path name (ex. ['','operators','dicom-reader','example_file.raw']
    name = file_name.split('/')

    # Check if file pertains to output directory (ex. "/operators)
    if name[1] == 'operators':

        # Download file to a local results directory to a file with same name on server (ex. example_file.raw)
        with open("./results/"+name[-1], 'wb+') as wb:
            payloads_client.download_from(payload_id=payload_id, blob_name="."+file_name, dest_obj=wb)

# Gets list of operator logs from job
jobs_logs = jobs_client.job_logs(job_id=job_id, operator_name="dicom-reader")
