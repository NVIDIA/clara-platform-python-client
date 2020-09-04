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
    packages=['nvidia_clara'],
    install_requires=['grpcio', 'protobuf'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        'License :: OSI Approved :: MIT License'
    ],
    python_requires='>=3.6',
)
