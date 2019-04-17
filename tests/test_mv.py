import itertools
import numpy as np
import os
import shutil
import subprocess
import unittest
import tempfile

from blockfs import Directory, Compression
from blockfs.test_utils import make_files
from blockfs.mv import main, copy_main

class TestMv(unittest.TestCase):
    def test_mv(self):
        for i in range(2):
            a = np.random.randint(0, 65535, (256, 256, 256), np.uint16)
            with make_files(1) as (dir_file, block_files):
                directory = Directory(256, 256, 256, np.uint16, dir_file,
                                      compression=Compression.zstd,
                                      block_filenames=block_files)
                directory.create()
                for x, y, z in itertools.product(range(0, 256, 64),
                                                 range(0, 256, 64),
                                                 range(0, 256, 64)):
                    directory.write_block(a[z:z+64, y:y+64, x:x+64], x, y, z)
                directory.close()
                dest = tempfile.mkdtemp()
                try:
                    if i == 0:
                        main([dir_file, dest])
                    else:
                        subprocess.check_call(["blockfs-mv", dir_file, dest])
                    dest_dir_file = \
                        os.path.join(dest, os.path.split(dir_file)[1])
                    dest_directory = Directory.open(dest_dir_file)
                    for x, y, z in itertools.product(range(0, 256, 64),
                                                     range(0, 256, 64),
                                                     range(0, 256, 64)):
                        block = dest_directory.read_block( x, y, z)
                        np.testing.assert_array_equal(
                            a[z:z+64, y:y+64, x:x+64], block)
                finally:
                    shutil.rmtree(dest)

    def test_cp(self):
        for i in range(2):
            a = np.random.randint(0, 65535, (256, 256, 256), np.uint16)
            with make_files(1) as (dir_file, block_files):
                directory = Directory(256, 256, 256, np.uint16, dir_file,
                                      compression=Compression.zstd,
                                      block_filenames=block_files)
                directory.create()
                for x, y, z in itertools.product(range(0, 256, 64),
                                                 range(0, 256, 64),
                                                 range(0, 256, 64)):
                    directory.write_block(a[z:z+64, y:y+64, x:x+64], x, y, z)
                directory.close()
                dest = tempfile.mkdtemp()
                try:
                    if i == 0:
                        copy_main([dir_file, dest])
                    else:
                        subprocess.check_call(["blockfs-cp", dir_file, dest])
                    dest_dir_file = \
                        os.path.join(dest, os.path.split(dir_file)[1])
                    for directory_file in dir_file, dest_dir_file:
                        dest_directory = Directory.open(directory_file)
                        for x, y, z in itertools.product(range(0, 256, 64),
                                                         range(0, 256, 64),
                                                         range(0, 256, 64)):
                            block = dest_directory.read_block( x, y, z)
                            np.testing.assert_array_equal(
                                a[z:z+64, y:y+64, x:x+64], block)
                finally:
                    shutil.rmtree(dest)

if __name__ == '__main__':
    unittest.main()
