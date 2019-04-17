import argparse
import os
import shutil
import sys
from .directory import Directory, Compression

EPILOG = """blockfs-mv moves the blockfs directory file and its block files
from their current location to the destination directory. The filenames will
remain the same and, if the destination directory does not exist, it will
be created. However, the parent directory of the destination directory
must already exist.
"""

def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description="Move a blockfs directory and its block files",
        epilog=EPILOG)
    parser.add_argument("source",
                        help="The directory file of the blockfs to be moved")
    parser.add_argument("dest_directory",
                        help="The directory to move to.")
    return parser.parse_args(args)


def main(args=sys.argv[1:], move=True):
    args = parse_args(args)
    directory = Directory.open(args.source)

    if not os.path.exists(args.dest_directory):
        os.mkdir(args.dest_directory)
    dest_block_filenames = [
        os.path.join(args.dest_directory, os.path.split(_)[1])
        for _ in directory.block_filenames]
    dest_directory_filename = os.path.join(
        args.dest_directory, os.path.split(directory.directory_filename)[1])
    dest_directory = Directory(
        x_extent = directory.x_extent,
        y_extent = directory.y_extent,
        z_extent = directory.z_extent,
        dtype = directory.dtype,
        directory_filename=dest_directory_filename,
        x_block_size=directory.x_block_size,
        y_block_size=directory.y_block_size,
        z_block_size=directory.z_block_size,
        block_filenames=dest_block_filenames,
        compression=getattr(Compression, directory.compression),
        compression_level=directory.compression_level,
        metadata=directory.metadata)
    dest_directory.create()
    src_length = os.stat(directory.directory_filename).st_size
    block_size = 2 ** 20
    with open(directory.directory_filename, "rb") as fd_src:
        with open(dest_directory_filename, "r+b") as fd_dest:
            fd_src.seek(directory.directory_offset, os.SEEK_SET)
            fd_dest.seek(dest_directory.directory_offset, os.SEEK_SET)
            for idx in range(directory.directory_offset,
                             src_length,
                             block_size):
                this_block_size = min(src_length - idx, block_size)
                a = fd_src.read(this_block_size)
                fd_dest.write(a)
    for src_block_path, dest_block_path in zip(
            directory.block_filenames,
            dest_block_filenames):
        os.remove(dest_block_path)
        if move:
            shutil.move(src_block_path, dest_block_path)
        else:
            shutil.copy(src_block_path, dest_block_path)
    if move:
        os.remove(directory.directory_filename)

def copy_main(args=sys.argv[1:]):
    main(args, move=False)
