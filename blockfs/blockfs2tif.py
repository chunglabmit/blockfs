import argparse
import itertools
import numpy as np
import os
import tqdm

from mp_shared_memory import SharedMemory
import multiprocessing
import sys
import tifffile

from .directory import Directory


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        help="The blockfs directory file",
        required=True)
    parser.add_argument(
        "--output-pattern",
        help="The pattern for TIFF file names, e.g. /path-to/img_%%04d.tiff "
        "where the field is the z coordinate of the TIFF plane"
    )
    parser.add_argument(
        "--n-workers",
        help="The number of worker processes",
        default=min(os.cpu_count(), 12),
        type=int)
    parser.add_argument(
        "--silent",
        help="Don't display progress bars",
        action="store_true"
    )
    parser.add_argument(
        "--compression",
        help="TIFF compression factor (0-9, default=3)",
        default=3,
        type=int
    )
    return parser.parse_args(args)


DIRECTORY = None


def read_block(shm, xoff, yoff, zoff, x0, x1, y0, y1, z0, z1):
    with shm.txn() as m:
        m[z0-zoff:z1-zoff, y0-yoff:y1-yoff, x0-xoff:x1-xoff] = \
            DIRECTORY.read_block(x0, y0, z0)


def write_plane(shm, path, z, compression):
    with shm.txn() as m:
        # More than 31 bits? Time to use bigtiff
        n_bits= np.log(m.dtype.itemsize * np.prod(m.shape)) / np.log(2)
        bigtiff =  n_bits > 31
        tifffile.imsave(path, m[z], compress=compression, bigtiff=bigtiff)


def main(args=sys.argv[1:]):
    global DIRECTORY
    opts = parse_args(args)
    DIRECTORY = Directory.open(opts.input)
    shm = SharedMemory(
        (DIRECTORY.z_block_size, DIRECTORY.y_extent, DIRECTORY.x_extent),
        DIRECTORY.dtype
    )
    dirnames = set()
    with multiprocessing.Pool(opts.n_workers) as pool:
        for z0 in range(0, DIRECTORY.z_extent, DIRECTORY.z_block_size):
            z1 = min(z0 + DIRECTORY.z_block_size, DIRECTORY.z_extent)
            yr = range(0, DIRECTORY.y_extent, DIRECTORY.y_block_size)
            xr = range(0, DIRECTORY.x_extent, DIRECTORY.x_block_size)
            futures = []
            for x0, y0 in itertools.product(xr, yr):
                x1 = min(x0 + DIRECTORY.x_block_size, DIRECTORY.x_extent)
                y1 = min(y0 + DIRECTORY.y_block_size, DIRECTORY.y_extent)
                futures.append(pool.apply_async(
                    read_block,
                    (shm, 0, 0, z0, x0, x1, y0, y1, z0, z1)))
            for future in tqdm.tqdm(futures,
                desc="Reading %d:%d" % (z0, z1),
                disable=opts.silent):
                future.get()
            futures = []
            for z in range(z0, z1):
                path = opts.output_pattern % z
                dirname = os.path.dirname(path)
                if dirname not in dirnames:
                    if not os.path.exists(dirname):
                        os.makedirs(dirname)
                    dirnames.add(dirname)
                futures.append(pool.apply_async(
                    write_plane, (shm, path, z - z0, opts.compression)))
            for future in tqdm.tqdm(
                futures,
                desc="Writing %d:%d" % (z0, z1),
                disable=opts.silent):
                future.get()


if __name__=="__main__":
    main()