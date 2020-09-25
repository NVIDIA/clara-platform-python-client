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


