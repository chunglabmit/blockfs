import unittest
from blockfs import Directory, Compression
from blockfs.test_utils import make_files
import numpy as np
from numcodecs import Blosc

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

    def test_02_01_encode_decode(self):
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  block_filenames=block_files)
            directory.create()
            try:
                test_cases = ((0, 524304), (524304, 524304))
                for offset, size in test_cases:
                    a = np.zeros(directory.directory_entry_size, np.uint8)
                    directory.encode_directory_entry(a, offset, size)
                    offset_out, size_out = directory.decode_directory_entry(a)
                    self.assertEqual(offset, offset_out)
                    self.assertEqual(size, size_out)
            finally:
                directory.close()

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

    def test_04_01_write2_read2(self):
        a = np.random.randint(0, 65535, (64, 64, 64), np.uint16)
        b = np.random.randint(0, 65535, (64, 64, 64), np.uint16)
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  compression=Compression.zstd,
                                  block_filenames=block_files)
            directory.create()
            directory.write_block(a, 64, 128, 192)
            directory.write_block(b, 0, 0, 0)
            directory.close()
            directory = Directory.open(dir_file)
            a_out = directory.read_block(64, 128, 192)
            np.testing.assert_array_equal(a, a_out)
            b_out = directory.read_block(0, 0, 0)
            np.testing.assert_array_equal(b, b_out)

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

    def test_06_write_using_array_interface(self):
        a = np.random.randint(0, 65535, (64, 64, 64), np.uint16)
        with make_files(1) as (dir_file, block_files):
            directory = Directory(1024, 1024, 1024, np.uint16, dir_file,
                                  compression=Compression.zstd,
                                  block_filenames=block_files)
            directory.create()
            directory[192:256, 128:192, 64:128] = a
            directory.close()
            a_out = Directory.open(dir_file).read_block(64, 128, 192)
            np.testing.assert_array_equal(a, a_out)

if __name__ == '__main__':
    unittest.main()
