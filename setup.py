from setuptools import setup

version="1.0.0"

with open("README.md", "r") as fd:
    long_description = fd.read()

console_scripts = [
            "blockfs-mv=blockfs.mv:main",
            "blockfs-cp=blockfs.mv:copy_main",
            "blockfs2tif=blockfs.blockfs2tif:main",
            "blockfs2jp2k=blockfs.blockfs2jp2k:main",
            "blockfs-rebase=blockfs.rebase:main"
    ]
#
# Optionally, make blockfs2hdf5 available if we have mpi4py installed
#
try:
    import mpi4py
    console_scripts.append("blockfs2hdf5=blockfs.blockfs2hdf5:main")
except:
    pass


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
    entry_points=dict(
        console_scripts = console_scripts
    ),
    install_requires=[
        "mp_shared_memory",
        "numpy",
        "numcodecs",
        "tifffile",
        "tqdm"
    ],
    extras_require={
        "JPEG2000": [ "glymur" ]
    }
)