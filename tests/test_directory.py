import unittest
from blockfs import Directory, Compression
import numpy as np
from numcodecs import Blosc
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


class TestDirectory(unittest.TestCase):
    def test_01_create(self):
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  block_filenames=block_files)
            directory.create()
            directory.close()
            with open(dir_file, "rb") as fd:
                header = fd.read(len(Directory.HEADER))
                self.assertEqual(header, Directory.HEADER)

    def test_02_create_and_open(self):
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  block_filenames=block_files)
            directory.create()
            directory.close()

            directory = Directory.open(dir_file)
            self.assertEqual(directory.x_extent, 1024)
            self.assertEqual(directory.y_extent, 1024)
            self.assertEqual(directory.z_extent, 1024)

    def test_03_write(self):
        a = np.random.randint(0, 65535, (64, 64, 64), np.uint16)
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  compression=Compression.zstd,
                                  block_filenames=block_files)
            directory.create()
            directory.write_block(a, 64, 128, 192)
            directory.close()

            with open(block_files[0], "rb") as fd:
                block = fd.read()
            blosc = Blosc("zstd")
            a_out = np.frombuffer(blosc.decode(block), np.uint16)\
               .reshape(64, 64, 64)
            np.testing.assert_array_equal(a, a_out)

    def test_04_write_read(self):
        a = np.random.randint(0, 65535, (64, 64, 64), np.uint16)
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  compression=Compression.zstd,
                                  block_filenames=block_files)
            directory.create()
            directory.write_block(a, 64, 128, 192)
            directory.close()
            a_out = Directory.open(dir_file).read_block(64, 128, 192)
            np.testing.assert_array_equal(a, a_out)

    def test_05_write_not_there(self):
        a = np.random.randint(0, 65535, (64, 64, 64), np.uint16)
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  compression=Compression.zstd,
                                  block_filenames=block_files)
            directory.create()
            directory.write_block(a, 64, 128, 192)
            directory.close()
            a_out = directory.read_block(192, 128, 64)
            np.testing.assert_array_equal(a_out, 0)
        #
        # Test for read in directory beyond EOF
        #
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  compression=Compression.zstd,
                                  block_filenames=block_files)
            directory.create()
            directory.write_block(a, 192, 128, 64)
            directory.close()
            a_out = directory.read_block(64, 128, 192)
            np.testing.assert_array_equal(a_out, 0)

if __name__ == '__main__':
    unittest.main()
