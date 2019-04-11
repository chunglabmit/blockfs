from setuptools import setup

version="1.0.0"

with open("README.md", "r") as fd:
    long_description = fd.read()

setup(
    name="blockfs",
    version=version,
    description="3D volume high-performance block storage",
    long_description=long_description,
    author="Kwanghun Chung Lab",
    packages=["blockfs"],
    url="https://github.com/chunglabmit/blockfs",
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        'Programming Language :: Python :: 3.5'
    ],
    install_requires=[
        "numpy",
        "numcodecs"
    ]
)