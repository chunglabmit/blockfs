# BlockFS

[![Travis CI Status](https://travis-ci.org/chunglabmit/blockfs.svg?branch=master)](https://travis-ci.org/chunglabmit/blockfs)

This is file-based storage for Neuroglancer 3D blocks. Design elements:

* Multithreaded write optimized for a FS with many disk spindles. No support
for anything but block writes in order to remove the necessity to read data
back into core. No worries about contention and locking. Multiple subprocesses
can write to the same FS at the same time.
* Simple disk reads of blocks
* Unabashedly focused on reading and writing blocks.
* Very few files / very large files

So simple to use:

```python
import numpy as np
from blockfs import Directory
import multiprocessing # not neccessary, demo its use though
import itertools

directory = Directory(1024, 1024, 1024, np.uint16, "volume.blockfs",
                      x_block_size=64, y_block_size=64, z_block_size=64)
directory.create()
directory.start_writer_processes() # need this for multithreading setup

def do_something_awesome(x, y, z):
    block = np.random.randint(0, 65535, (64, 64, 64), np.uint16)
    directory.write_block(block, x, y, z)

with multiprocessing.Pool() as pool:
    pool.starmap(do_something_awesome, itertools.product(range(0, 1024, 64),
                                                         range(0, 1024, 64),
                                                         range(0, 1024, 64)))
directory.close()

block_128_64_0 = Directory.open("volume.blockfs").read_block(128, 64, 0)

```

Works in conjunction with [precomputed-tif](https://github.com/chunglabmit/precomputed-tif)

## Utilities

```
blockfs-mv <src> <dest>
```
Moves a blockfs directory file and its block files to a new
location. The source should be a blockfs directory file and
the destination should be the target directory for the files.
The utility will move the block files and rewrite the directory
file to point at the new block file locations.

It's necessary to use blockfs-mv instead of mv because the file names
of the block files are embedded within the directory file.

* **src** - the name of the directory file of the blockfs to be moved
* **dest** - the name of the filesystem target directory for the new
             locations of the files 
```
blockfs-cp <src> <dest>
```
Copies a blockfs directory file. Same as blockfs-mv except does
not (re)move the files.

## blockfs2tif

*blockfs2tif* converts a blockfs volume to TIFF stacks.

Usage:

```bash
blockfs2tif \
    --input <blockfs-file> \
    --output-pattern <output-pattern> \
    [--n-workers <n-workers>] \
    [--silent] \
    [--compression <compression>]
```

where
* *blockfs-file* is the index file of the blockfs, e.g.
  precomputed.blockfs
  
* *output-pattern* is the naming convention for .tiff files, e.g.
  /path/to/img_%04d.tiff
  
* *n-workers* is the number of worker processes to use

* *silent* will prevent display of tqdm progress bar

* *compression* compression level = 0 to 9

## blockfs2hdf

**Note: only available if parallel HDF is built for current
environment**

*blockfs2hdf* converts a blockfs volume to HDF5.

Usage:
```bash
mpiexec -n <n-workers> blockfs2hdf \
    <src> \
    <dest> \
    <name> \
    [--compression <compression>] \
    [--compression-opts <compression-opts>]
```

where
* *n-workers* is the number of worker processes to use
  
* *src* is the blockfs index file, e.g. precomputed.blockfs

* *dest* is the name of the HDF5 file. It must already have been
  created.
  
* *name* is the name of the dataset to be created.

* *compression* is the name of the compression to use, e.g. "lzf".
  By default, *blockfs2hdf* uses GZIP.
  
* *compression-opts* gives the compression options for the compression,
  such as a number for gzip
  
## blockfs2jp2k

*blockfs2jp2k* converts a blockfs volume to JPEG 2000 stacks.

Note:
*blockfs2jp2k* is only available if you have installed the optional
dependency, "glymur".

Usage:

```bash
blockfs2jp2k \
    --input <blockfs-file> \
    --output-pattern <output-pattern> \
    [--n-workers <n-workers>] \
    [--silent] \
    [--psnr <psnr>]
```

where
* *blockfs-file* is the index file of the blockfs, e.g.
  precomputed.blockfs
  
* *output-pattern* is the naming convention for .jp2 files, e.g.
  /path/to/img_%04d.jp2
  
* *n-workers* is the number of worker processes to use

* *silent* will prevent display of tqdm progress bar

* *psnr* this is the signal-to-noise ratio for data loss for lossy
  compression, measured in DB. Higher numbers result in less loss
  and 80dB, for instance, yields an image with little visible
  difference from the original. The default is lossless compression.

## blockfs-rebase

**blockfs-rebase** fixes up the blockfs directory file after it and the block files
have been moved to a new directory.

Usage:

blockfs-rebase [--block-size <block-size>] <blockfs-file>

where:

* **blockfs-file** is the path to the precomputed.blockfs file

* **block-size** is the number of bytes in each read of the blockfs index file.

