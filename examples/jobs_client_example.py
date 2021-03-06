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

from nvidia_clara.jobs_client import JobsClient
import nvidia_clara.job_types as job_types
import nvidia_clara.pipeline_types as pipeline_types

# Client Creation with IP and Port of running instance of Clara
clara_ip_address = "10.0.0.1"
clara_port = "30031"

jobs_client = JobsClient(target=clara_ip_address, port=clara_port)

# Creates Filter of Healthy Jobs - Additionally could filter by Pipeline Id, State, Completion Time, and Creation Time
job_filter = job_types.JobFilter(has_job_status=[job_types.JobStatus.Healthy])

# List Current Jobs with Optional Filter
job_list = jobs_client.list_jobs(job_filter=job_filter)
print(job_list)

# Identifier of created pipeline (ex. colon tumor segmentation)
colon_tumor_pipeline_id = "f9a843935e654a30beb9d1b8352bfaac"

# Create Job
job_info = jobs_client.create_job(job_name="colontumor",pipeline_id=pipeline_types.PipelineId(colon_tumor_pipeline_id))
print(job_info.job_id.value)

# Start Job
job_token = jobs_client.start_job(job_id=job_info.job_id)
print(job_token.job_state)
print(job_token.job_status)

# Get Status of Job from Identifier
job_details = jobs_client.get_status(job_id=job_token.job_id)

print(job_details.job_state)
print(job_details.job_status)

# Gets List of Operators
print(job_details.operator_details.keys())

# Try Canceling Job (if still running)
try:
    job_details = jobs_client.cancel_job(job_id=job_token.job_id)
except:
    print("Scheduler Rejected Request")


