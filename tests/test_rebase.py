import itertools
import tempfile
import traceback

import numpy as np
import pathlib
import unittest

from blockfs import Directory, Compression
from blockfs.rebase import main


class TestRebase(unittest.TestCase):
    def test_case(self):
        src_dir = pathlib.Path(tempfile.mkdtemp())
        dest_dir_parent = pathlib.Path(tempfile.mkdtemp())
        dest_dir = dest_dir_parent / "dest"
        dest_dir.mkdir()
        all_files = []
        rs = np.random.RandomState(1234)
        a = rs.randint(0, 65535, (256, 256, 256), dtype=np.uint16)
        try:
            dir_file = src_dir / "my.blockfs"
            block_files = [src_dir / ("my.blockfs.%d" % i) for i in range(4)]
            directory = Directory(256, 256, 256, np.uint16, str(dir_file),
                                  compression=Compression.zstd,
                                  block_filenames=
                                  [str(_) for _ in block_files])
            directory.create()
            for x, y, z in itertools.product(range(0, 256, 64),
                                             range(0, 256, 64),
                                             range(0, 256, 64)):
                directory.write_block(
                    a[z:z + 64, y:y + 64, x:x + 64], x, y, z)
            directory.close()
            for path in [dir_file] + list(block_files):
                dest_path = dest_dir / path.name
                path.replace(dest_path)
                all_files.append(dest_path)
            dest_directory_file = dest_dir / pathlib.Path(dir_file).name
            main([str(dest_directory_file)])
            directory = Directory.open(str(dest_directory_file))
            for x, y, z in itertools.product(range(0, 256, 64),
                                             range(0, 256, 64),
                                             range(0, 256, 64)):
                np.testing.assert_array_equal(a[z:z+64, y:y+64, x:x+64],
                                              directory.read_block(x, y, z))
        finally:
            try:
                for path in all_files:
                    path.unlink()
                dest_dir.rmdir()
                dest_dir_parent.rmdir()
            except:
                traceback.print_exc()
                print("Failed to remove files")


if __name__ == '__main__':
    unittest.main()
