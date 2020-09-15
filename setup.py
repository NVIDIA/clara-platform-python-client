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

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ClaraClient",
    version="0.7.0",
    author="Clara Deploy",
    description="Python package to interact with Clara Platform Server API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab-master.nvidia.com/Clara/sdk/-/tree/master/Platform/Client_Python",
    install_requires=['grpcio', 'protobuf'],
    packages=setuptools.find_packages('.'),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        'License :: OSI Approved :: MIT License'
    ],
    python_requires='>=3.6',
)
