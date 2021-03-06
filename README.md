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