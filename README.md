[![License](https://img.shields.io/badge/License-Apache_2.0-lightgrey.svg)](https://opensource.org/licenses/Apache-2.0)
[![Clara Deploy Platform](https://img.shields.io/badge/Clara_Deploy_Platform-0.7.1-brightgreen.svg)](https://ngc.nvidia.com/catalog/containers/nvidia:clara:platformapiserver)

[![NVIDIA](https://github.com/NVIDIA/clara-platform-python-client/blob/main/ext/NVIDIA_horo_white.png?raw=true)](https://docs.nvidia.com/clara/deploy/index.html)

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


## Getting Started

### Package Installation
#### Installing from [source repository](https://github.com/NVIDIA/clara-platform-python-client)
```
$ git clone --recursive git@github.com:kubernetes-client/python.git
$ cd ./clara-platform-python-client
$ python3 -m pip install .
```

#### Installing directly from [Pypi](https://pypi.org/project/nvidia-clara-client/)
```
$ python3 -m pip install nvidia-clara-client
```

### Clara Client Guides
* [Jobs](https://github.com/NVIDIA/clara-platform-python-client/wiki/Jobs-Client)
: Learn to start and manage Clara jobs
* [Pipelines](https://github.com/NVIDIA/clara-platform-python-client/wiki/Pipelines-Client)
: Learn to create and manage Clara pipelines
* [Payloads](https://github.com/NVIDIA/clara-platform-python-client/wiki/Payloads-Client)
: Learn to create, upload, download, and manage Clara payloads

### Full  Example(s) Running Pipeline
* [Spleen Segmentation Pipeline](https://github.com/NVIDIA/clara-platform-python-client/wiki/Spleen-Segmentation-Example)


## Running Pytests
*Only for developing with source repository*
```
$ pip3 install grpcio-testing
$ pip3 install pytest
$ export PYTHONPATH="${PYTHONPATH}:<INSERT PATH TO /clara-platform-python-client>"
$ pytest <INSERT PATH TO /clara-platform-python-client>
```