# Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
#
# NVIDIA CORPORATION and its licensors retain all intellectual property
# and proprietary rights in and to this software, related documentation
# and any modifications thereto.  Any use, reproduction, disclosure or
# distribution of this software and related documentation without an express
# license agreement from NVIDIA CORPORATION is strictly prohibited.

from nvidia_clara.jobs_client import JobsClient
import nvidia_clara.pipeline_types as pipeline_types

# Client Creation with IP and Port of running instance of Clara
clara_ip_address = "10.0.0.1"
clara_port = "31851"

jobs_client = JobsClient(target=clara_ip_address, port=clara_port)

# List Current Jobs
job_list = jobs_client.list_jobs()
print(job_list)

# Identifier of created pipeline (ex. HQS)
hqs_pipeline_id = "f9a843935e654a30beb9d1b8352bfaac"

# Create Job
job_info = jobs_client.create_job(job_name="hqstest",pipeline_id=pipeline_types.PipelineId(hqs_pipeline_id))
print(job_info.job_id.value)

# Start Job
job_token = jobs_client.start_job(job_id=job_info.job_id)
print(job_token.job_state)
print(job_token.job_status)

# Get Status of Job from Identifier
job_details = jobs_client.get_status(job_id=job_token.job_id)

print(job_details.job_state)
print(job_details.job_status)

# Try Canceling Job (if still running)
try:
    job_details = jobs_client.cancel_job(job_id=job_token.job_id)
except:
    print("Scheduler Rejected Request")


