import argparse
import pathlib
import sys

from blockfs import Directory

DESCRIPTION = """blockfs-rebase rewrites the index file after it and the
shard files have been moved to a different directory (or after the directory
has been renamed.
"""
def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description=DESCRIPTION)
    parser.add_argument(
        "index_file",
        help="The name of the .blockfs index file to be rewritten"
    )
    parser.add_argument(
        "--block-size",
        help="Size of a block when copying the index data",
        default=4096 * 16,
        type=int
    )
    return parser.parse_args(args)


def main(args=sys.argv[1:]):
    opts = parse_args(args)
    src_path = pathlib.Path(opts.index_file).absolute().resolve()
    src_directory = Directory.open(str(src_path))
    directory_offset = src_directory.directory_offset
    dest_path = src_path.parent / (src_path.name + '.new')
    src_directory.block_filenames = [
        str(src_path.parent / pathlib.Path(_).name)
        for _ in src_directory.block_filenames
    ]
    src_directory.directory_filename = str(dest_path)
    src_directory.create(create_shards=False)
    with src_path.open("rb") as src_fd:
        src_fd.seek(directory_offset)
        with dest_path.open("ab+") as dest_fd:
            for offset in range(src_directory.directory_offset,
                                src_path.stat().st_size,
                                opts.block_size):
                data = src_fd.read(opts.block_size)
                dest_fd.write(data)
    dest_path.replace(src_path)


