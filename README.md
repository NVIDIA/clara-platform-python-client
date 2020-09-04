[![NVIDIA](https://github.com/NVIDIA/clara-platform-python-client/blob/master/ext/NVIDIA_horo_white.png?raw=true)](https://docs.nvidia.com/clara/deploy/index.html)

# Clara Deploy Python Client
An intuitive python 3 package to develop applications with NVIDIA Clara Deploy. Utilize the clients within the **nvidia_clara** package to manage jobs, pipelines, payloads, and models. Each client has an associated set of objects which are defined in seperate 'types' modules (also can be found in nvidia_clara). Look at the examples below to learn more on each client to get started!

### Additional Resources to Learn More on Clara Deploy
* [NVIDIA Clara Overview Homepage](https://developer.nvidia.com/clara)
* [NVIDIA Clara Deploy SDK User Guide](https://docs.nvidia.com/clara/deploy/index.html)


### Client Prerequisites
* Python 3.6 or higher
* Clara Deploy 0.7.0 or higher

### Pypi Packages Needed
* [Grpcio](https://pypi.org/project/grpcio)
* [Protobuf](https://pypi.org/project/protobuf)
* [Grpcio-Testing (For Running Pytests)](https://pypi.org/project/grpcio-testing)
* [Pytest (For Running Pytests)](https://pypi.org/project/pytest)

### Getting Started
```
$ sudo pip3 install <Insert Path For /clara-platform-python-client>
```
or
```
$ export PYTHONPATH=<Local path to /python-api-client Directory>
$ pip3 install grpcio
$ pip3 install protobuf
```

### Jobs Client Example

```python
>>> from nvidia_clara.jobs_client import JobsClient
>>> import nvidia_clara.pipeline_types as pipeline_types

# Client Creation with IP and Port of running instance of Clara
>>> clara_ip_address = "10.0.0.1"
>>> clara_port = "31851"

>>> jobs_client = JobsClient(target=clara_ip_address, port=clara_port)

# List Current Jobs
>>> job_list = jobs_client.list_jobs()
[<job_types.JobInfo object at 0x058908E0>, <job_types.JobInfo object at 0x063208E0>]

# Identifier of created pipeline (ex. HQS)
>>> hqs_pipeline_id = "dd05c5b79461402cb0a610d4f0fce36f"

# Create Job
>>> job_info = jobs_client.create_job(job_name="hqstest",pipeline_id=pipeline_types.PipelineId(hqs_pipeline_id))
<job_types.JobInfo object at 0x05369B78>

>>> job_info.job_id.value
'3a4edd810b4b4945be4d34c4c57bfb67'

# Start Job
>>> job_token = jobs_client.start_job(job_id=job_info.job_id)
<job_types.JobToken object at 0x05373B78>

>>> job_token.job_state
1

>>> job_token.job_status
1

# Get Status of Job from Identifier
>>> job_details = jobs_client.get_status(job_id=job_token.job_id)
<job_types.JobDetails object at 0x05373B78>

>>> job_details.job_state
1

>>> job_details.job_status
1

# Try Canceling Job (if still running)
>>> job_details = jobs_client.cancel_job(job_id=job_token.job_id)

```

### Pipelines Client Example

```python
>>> from pathlib import Path
>>> from nvidia_clara.pipelines_client import PipelinesClient
>>> import nvidia_clara.pipeline_types as pipeline_types

# Client Creation with IP and Port of running instance of Clara
>>> clara_ip_address = "10.0.0.1"
>>> clara_port = "31851"

>>> pipeline_client = PipelinesClient(target=clara_ip_address, port=clara_port)

# Create list of pipeline_types.PipelineDefinition with local path to pipeline .yaml
>>> file_path = "./liver-tumor-pipeline.yaml"
>>> definitions = [pipeline_types.PipelineDefinition(name=file_path, content=Path(file_path).read_text())]

# Create Pipeline with definition list created
>>> pipeline_id = pipeline_client.create_pipeline(definition=definitions)
<pipeline_types.PipelineId object at 0x05369B78>

>>> pipeline_id.value
'3fd032fcb1a441a1936654b58b1df43b'

# Get List of Created Pipelines PipelinesClient.list_pipelines()
>>> pipelines = [(pipe_info.pipeline_id.value, pipe_info.name) for pipe_info in pipeline_client.list_pipelines()]
[('3fd032fcb1a441a1936654b58b1df43b', 'liver-tumor-pipeline'), ('a4ea69a01a434ad09d05d2e6bf362e70', 'dicom-io-example'), ('e62b8d0207aa4d20b1e4aedd629902c7', 'dicom-io-example')]

# Get Details of Pipeline with PipelinesClient.pipeline_details()
>>> pipeline_details = pipeline_client.pipeline_details(pipeline_id=pipeline_id)
<pipeline_types.PipelineDetails object at 0x05CB6F88>

# Remove Pipeline
>>> pipeline_client.remove_pipeline(pipeline_id=pipeline_id)
```

### Payloads Client Example</h3></title>

```python
>>> from payloads_client import PayloadsClient
>>> import nvidia_clara.payload_types as payload_types

# Client Creation with IP and Port of running instance of Clara
>>> clara_ip_address = "10.0.0.1"
>>> clara_port = "31851"

>>> payload_client = PayloadsClient(target=clara_ip_address, port=clara_port)

# Create static Payload
>>> payload_details = payload_client.create_payload()
<payload_types.PayloadDetails object at 0x05F2CF28>

>>> payload_details.payload_id.value
'a27b8ab967f04582855b7cc4a702e6d3'

# Delete Payload
>>> payload_client.delete_payload(payload_id=payload_details.payload_id)

# Create BinaryIO stream object with write permissions and download from payload identifier: example_payload_identifier
>>> with open('output.dcm', 'wb') as wb:
...     payload_client.download_from(payload_id=payload_types.PayloadId(example_payload_identifier),
...                                  blob_name='./input/I114.dcm',
...                                  dest_obj=wb)

# Uploading BinaryIO stream to a new blob
# Create BinaryIO stream with read permissions (for sake of example: reading previous output stream)
>>> with open('output.dcm', 'rb') as rb:
...     payload_client.upload(payload_id=payload_types.PayloadId(example_payload_identifier),
...                           blob_name='./test/new_blob.dcm', file_object=rb)

# Get Details (including List of Files) of a Payload
>>> get_details = payload_client.get_details(payload_id=payload_types.PayloadId(example_payload_identifier))
<payload_types.PayloadDetails object at 0x0573DF28>

>>> file_details = get_details.file_details
[<clara_client.payload_types.PayloadFileDetails object at 0x05AFFF88>, <clara_client.payload_types.PayloadFileDetails object at 0x05AF1FE8>, ... ]

>>> file_details[0].mode
0

>>> file_details[0].name
'/input/I101.dcm'

>>> file_details[0].size
525414
```

### Spleen Segmentation Combined Example - Creating Pipeline/Job and Uploading/Downloading Payload</h3></title>

```python
>>> from nvidia_clara.jobs_client import JobsClient
>>> from nvidia_clara.pipelines_client import PipelinesClient
>>> from nvidia_clara.payloads_client import PayloadsClient
>>> import nvidia_clara.pipeline_types as pipeline_types
>>> import os
>>> from pathlib import Path

# Clients creation
>>> clara_ip_address = "10.0.0.1"
>>> clara_port = "30407"

>>> jobs_client = JobsClient(target=clara_ip_address, port=clara_port)
>>> payloads_client = PayloadsClient(target=clara_ip_address, port=clara_port)
>>> pipelines_client = PipelinesClient(target=clara_ip_address, port=clara_port)

# Create list of pipeline_types.PipelineDefinition with local path to pipeline .yaml
>>> file_path = "./spleen_pipeline.yaml"
>>> definitions = [pipeline_types.PipelineDefinition(name=file_path, content=Path(file_path).read_text())]

# Create Pipeline with definition list created
>>> pipeline_id = pipelines_client.create_pipeline(definition=definitions)

# Create Job with newly created Pipeline
>>> job_info = jobs_client.create_job(job_name="spleenjob", pipeline_id=pipeline_types.PipelineId(pipeline_id.value))
>>> job_id = job_info.job_id
>>> payload_id = job_info.payload_id

# Local path to directory of files to upload to the job's payload on the Server
>>> input_path = "./app_spleen-input_v1/dcm"

# Go through files in directory and upload to the job using the payload identifier
>>> for file in os.listdir(input_path):
...     file_path = input_path + "/" + str(file)
...     with open(file_path, 'rb') as fp:
...         payloads_client.upload(payload_id=payload_id, blob_name=file, file_object=fp)

# Get a List of the jobs
>>> job_list = jobs_client.list_jobs()

# Start Job
>>> job_token = jobs_client.start_job(job_id=job_id)

# Loop until job completes
>>> job_status = jobs_client.get_status(job_id=job_id)
>>> while job_status.job_state != 3:
...     job_status = jobs_client.get_status(job_id=job_id)

# Get Payload Details - Used to get list of payload files
>>> payload_details = payloads_client.get_details(payload_id=payload_id)

# Download files from payload if pertaining to output payload directory (ex. "/operators)
>>> for file in payload_details.file_details:
...
...     # Get file path on Server (ex. /operators/dicom-reader/example_file.raw")
...     file_name = file.name
...
...     # Split file path name (ex. ['','operators','dicom-reader','example_file.raw'])
...     name = file_name.split('/')
...
...     # Check if file pertains to output directory (ex. "/operators")
...     if name[1] == 'operators':
...
...         # Download file to a local results directory to a file with same name on server (ex. example_file.raw)
...         with open("./results/"+name[-1], 'wb+') as wb:
...             payloads_client.download_from(payload_id=payload_id, blob_name="."+file_name, dest_obj=wb)


# Gets list of operator logs from job
>>> jobs_logs = jobs_client.job_logs(job_id=job_id, operator_name="dicom-reader")
```

### Running Pytests
```
$ pip3 install grpcio-testing
$ pip3 install pytest
$ pytest
```
