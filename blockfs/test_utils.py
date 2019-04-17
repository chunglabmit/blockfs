# Utilities for testing

import tempfile
import contextlib
import os


@contextlib.contextmanager
def make_files(n_block_files):
    dir_handle, dir_file = tempfile.mkstemp(".blockfs")
    block_files = []
    block_handles = []
    for i in range(n_block_files):
        bh, bf = tempfile.mkstemp(".blockfs.%d" % i)
        block_files.append(bf)
        block_handles.append(bh)
    yield dir_file, block_files
    os.close(dir_handle)
    try:
        os.remove(dir_file)
    except IOError:
        print("Warning: failed to remove %s" % dir_file)
    for block_file, block_handle in zip(block_files, block_handles):
        os.close(block_handle)
        try:
            os.remove(block_file)
        except IOError:
            print("Warning: failed to remove %s" % block_file)

