import argparse
import itertools
from mpi4py import MPI
import h5py
from .directory import Directory
import sys
import logging
import tqdm

comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

logging.basicConfig(level=logging.INFO if rank==0 else logging.ERROR)

def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description=
        "Copy a blockfs volume to HDF5 using parallel HDF5. Usage is "
        '"mpiexec -n ? blockfs2hdf5 <src> <dest> <name>"')
    parser.add_argument("src",
                        help="precomputed.blockfs file to be copied")
    parser.add_argument(
        "dest",
        help="HDF5 file to be created.")
    parser.add_argument(
        "name",
        help="The name of the HDF5 dataset within the HDF file. This can be "
        "a pathname")
    parser.add_argument(
        "--compression",
        help="Compression type: gzip, lzf or other used to create the dataset",
        default="gzip")
    parser.add_argument(
        "--compression-opts",
        help="Compression options: see h5py documentation for options")
    return parser.parse_args(args)


def main(args=sys.argv[1:]):
    opts = parse_args(args)
    directory = Directory.open(opts.src)
    block_size = directory.get_block_size(0, 0, 0)
    logging.info("Opened %s" % opts.src)
    f = h5py.File(opts.dest, "a", driver="mpio", comm=comm)
    logging.info("Opened %s" % opts.dest)
    cd_args = dict(shape=directory.shape,
                   chunks=block_size,
                   dtype=directory.dtype,
                   compression=opts.compression)
    if opts.compression_opts is not None:
        cd_args["compression_opts"] = opts.compression_opts
    ds = f.create_dataset(opts.name, **cd_args)
    logging.info("Created dataset %s" % opts.name)
    zs, ys, xs = directory.get_block_size(0, 0, 0)
    if rank == 0:
        work = list(itertools.product(range(0, directory.shape[2], xs),
                                      range(0, directory.shape[1], ys),
                                      range(0, directory.shape[0], zs)))
        work = [work[i::size] for i in range(size)]
    else:
        work = None
    work = comm.scatter(work, root=0)
    for x0, y0, z0 in tqdm.tqdm(work, disable=rank > 0):
        block = directory.read_block(x0, y0, z0)
        with ds.collective:
            ds[z0:z0+block.shape[0],
            y0:y0+block.shape[1],
            x0:x0+block.shape[2]] = block
    comm.Barrier()
    f.close()


if __name__=="__main__":
    main()

