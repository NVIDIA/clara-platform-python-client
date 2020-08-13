# Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
#
# NVIDIA CORPORATION and its licensors retain all intellectual property
# and proprietary rights in and to this software, related documentation
# and any modifications thereto.  Any use, reproduction, disclosure or
# distribution of this software and related documentation without an express
# license agreement from NVIDIA CORPORATION is strictly prohibited.

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

