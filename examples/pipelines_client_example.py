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

from pathlib import Path
from nvidia_clara.pipelines_client import PipelinesClient
import nvidia_clara.pipeline_types as pipeline_types

# Client Creation with IP and Port of running instance of Clara

clara_ip_address = "10.0.0.1"
clara_port = "31851"

pipeline_client = PipelinesClient(target=clara_ip_address, port=clara_port)

# Create list of pipeline_types.PipelineDefinition with local path to pipeline .yaml
file_path = "./liver-tumor-pipeline.yaml"
definitions = [pipeline_types.PipelineDefinition(name=file_path, content=Path(file_path).read_text())]

# Create Pipeline with definition list created
pipeline_id = pipeline_client.create_pipeline(definition=definitions)
print(pipeline_id)

# Get List of Created Pipelines PipelinesClient.list_pipelines()
pipelines = [(pipe_info.pipeline_id.value, pipe_info.name) for pipe_info in pipeline_client.list_pipelines()]
print(pipelines)

# Get Details of Pipeline with PipelinesClient.pipeline_details()
pipeline_details = pipeline_client.pipeline_details(pipeline_id=pipeline_id)

# Remove Pipeline
pipeline_client.remove_pipeline(pipeline_id=pipeline_id)

