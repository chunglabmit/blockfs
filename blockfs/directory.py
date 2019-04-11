'''BlockFS directory

The directory keeps track of the blockfiles and the extents of the
volume. The format is:

           Size      Description
           (bytes)
Header     8         The text "BlockFS" zero-terminated
MDSize     4         The length of the JSON metadata
DirOffset  4         Offset to the first directory entry.
Metadata   variable  A block of JSON-encoded metadata dictionary.
Directory  M * N     One entry per block giving the file offset of that block

JSON Metadata
XBlockSize     (uint32): size of one block in the X direction
YBlockSize     (uint32): size of one block in the Y direction
ZBlockSize     (uint32): size of one block in the Z direction
NOffsetBits    (uint32): # of bits in a directory entry devoted to the offset
                         to the entry in the file
NSizeBits      (uint32): # of bits in a directory entry devoted to the size
                         of the compressed block
XExtent        (uint32): extent of volume in the X direction in voxels
YExtent        (uint32): extent of the volume in the Y direction in voxels
ZExtent        (uint32): extent of the volume in the Z direction in voxels
DType          (str): the Numpy dtype name
BlockFilenames (list of strings): filenames of block files
XStride        (uint32): stride in X direction in directory entries (bytes * 8)
YStride        (uint32): stride in Y direction in directory entries (bytes * 8)
ZStride        (uint32): stride in Z direction in directory entries (bytes * 8)
Compression    (str): one of the BLOSC compression schemes
CompressionLvl (uint32): Compression level (0 - 9)
Version        (str): a dotted protocol version string: 1.0.0
Other application-specific metadata can be stored as well

A reader computes the offset in the directory and file as follows
offset = ((x // XBlockSize) * XStride + (y // YBlockSize) * YStride +
          (z // ZBlockSize) * ZStride))
file_index = offset % (len(BlockFilenames))
directory_entry_offset = DirOffset + offset * (NOffsetBits + NSizeBits) // 8
filename = BlockFilenames[file_index]
'''

import enum
import io
import json
import numpy as np
import os
from numcodecs import Blosc
import multiprocessing
import threading
from .writer import BlockWriter


class Compression(enum.Enum):
    """Compression type for Blosc"""
    zstd=1
    blosclz=2
    lz4=3
    lz4hc=4
    zlib=5
    snappy=6


class Directory:

    HEADER = b"BlockFS\0"
    CURRENT_VERSION = "1.0.0"

    def __init__(self, x_extent, y_extent, z_extent, dtype,
                 directory_filename,
                 n_filenames=os.cpu_count(),
                 x_block_size=64, y_block_size=64, z_block_size=64,
                 n_offset_bits=None, n_size_bits=None,
                 directory_offset=None,
                 block_filenames=None,
                 x_stride=None, y_stride=None, z_stride=None,
                 compression=Compression.zstd,
                 compression_level=5,
                 metadata=None):
        """
        Initialize the directory with what it needs to read or write blockfs

        :param x_extent: the size of the volume in the X direction
        :param y_extent: the size of the volume in the Y direction
        :param z_extent: the size of the volume in the Z direction
        :param dtype: the numpy datatype
        :param directory_filename: the path to the directory file
        :param n_filenames: the number of block files to create
        :param x_block_size: the size of one block in the X direction
        :param y_block_size: the size of one block in the Y direction
        :param z_block_size: the size of one block in the Z direction
        :param n_offset_bits: the number of bits of offset in a directory entry
        :param n_size_bits: the number of bits of block size in a dir entry
        :param directory_offset: the file offset (from 0) of the directory
        :param block_filenames: the names of the block files
        :param x_stride: the offset stride in the X direction
        :param y_stride: the offset stride in the Y direction
        :param z_stride: the offset stride in the Z direction
        :param compression: the kind of compression used
        :param compression_level: the level of compression
        :param metadata: metadata dictionary to include in the directory file
        """
        self.directory_filename = directory_filename
        self.x_extent = x_extent
        self.y_extent = y_extent
        self.z_extent = z_extent
        self.dtype = np.dtype(dtype)
        self.x_block_size = x_block_size
        self.y_block_size = y_block_size
        self.z_block_size = z_block_size
        if block_filenames is None:
            block_filenames = \
                ["%s.%d" % (directory_filename, _) for _ in range(n_filenames)]
        else:
            n_filenames = len(block_filenames)
        self.block_filenames = block_filenames
        if x_stride is None:
            x_stride = 1
        if y_stride is None:
            y_stride = (x_extent + x_block_size - 1) // x_block_size
        if z_stride is None:
            z_stride = (y_stride * y_extent + y_block_size - 1) // y_block_size
        self.x_stride = x_stride
        self.y_stride = y_stride
        self.z_stride = z_stride
        if n_offset_bits is None:
            last_offset = self.offsetof(x_extent + x_block_size - 1,
                                        y_extent + y_block_size - 1,
                                        z_extent + z_block_size - 1)
            n_offset_bits = int(np.log(last_offset) / np.log(2) + 1)
        if n_size_bits is None:
            # Blosc has a header of size 16 and will use a passthrough encoding
            # if a block "compresses" to more than the size of the data
            #
            largest_size = \
                x_block_size * y_block_size * z_block_size * \
                self.dtype.itemsize + 16
            n_size_bits = int(np.log(largest_size) / np.log(2) + 1)
        self.n_offset_bits = n_offset_bits
        self.n_size_bits = n_size_bits
        self.directory_entry_size = (n_offset_bits + n_size_bits + 7) // 8
        self.compression = compression
        self.compression_level = compression_level
        if metadata is None:
            metadata = {}
        self.metadata = metadata
        self.directory_offset = directory_offset
        self.upqueue = multiprocessing.Queue()
        self.writers = None
        self.directory_writer = None

    def start_writer_processes(self, queue_depth=10):
        """
        Start writer processes. In a multiprocessing scenario, it's useful
        to do this before the fork so that subprocesses can use the queues.

        It's not necessary to call this in a single-process model. It will
        be called upon the first write.

        :param queue_depth: The maximum number of chunks that can be enqueued
        for any one writer. Max memory enqueued is x_block_size * y_block_size *
        z_block_size * len(block_filenames) * queue_depth * dtype.itemsize
        """
        if self.writers is not None:
            return
        self.writers = [BlockWriter(block_filename, self.upqueue,
                                    self.compression.name,
                                    self.compression_level,
                                    queue_depth=queue_depth)
                        for block_filename in self.block_filenames]
        self.directory_writer = threading.Thread(
            target=Directory.directory_writer_process,
            args=(self,))
        self.directory_writer.start()

    def close(self):
        if self.writers is None:
            return
        for writer in self.writers:
            writer.close()
        self.upqueue.put(None)
        self.directory_writer.join()
        self.writers = None

    def offsetof(self, x, y, z):
        """
        Offset of a
        :param x:
        :param y:
        :param z:
        :return:
        """
        return self.x_stride * (x // self.x_block_size) + \
               self.y_stride * (y // self.y_block_size) + \
               self.z_stride * (z // self.z_block_size)

    def encode_directory_entry(self, a, offset, size):
        """
        Encode a directory entry into an array or view

        :param a: byte array or view positioned to the location for encoding
        :param offset: the offset to encode
        :param size: the size to encode
        """
        offset_and_size = offset + size * 2 ** self.n_offset_bits
        for idx in range(self.directory_entry_size):
            a[idx] = offset_and_size & 0xff
            offset_and_size = offset_and_size >> 8

    def decode_directory_entry(self, a):
        """
        Return the offset and size, decoded from a directory entry

        :param a: an array or view of a directory entry
        :return: a two tuple of offset and size
        """
        accumulator = 0
        pow = 1
        for b in a:
            accumulator = accumulator + int(b) * pow
            pow = pow * 256
        offset = accumulator & ((1 << self.n_offset_bits) - 1)
        size = (accumulator >> self.n_offset_bits) & \
               ((1 << self.n_size_bits) - 1)
        return offset, size

    @staticmethod
    def open(directory_filename):
        """
        Open the given directory

        :param directory_filename: the path to the directory file
        :return: a directory structure
        """
        with open(directory_filename, "rb") as fd:
            header = fd.read(8)
            if header != Directory.HEADER:
                raise IOError("%s is not a BlockFS file" % directory_filename)
            md_size, dir_offset = np.frombuffer(fd.read(8), np.uint32)
            metadata = json.loads(fd.read(md_size), encoding="UTF-8")
            application_metadata = {}
            for key, value in metadata.items():
                if key == "XBlockSize":
                    x_block_size = int(value)
                elif key == "YBlockSize":
                    y_block_size = int(value)
                elif key == "ZBlockSize":
                    z_block_size = int(value)
                elif key == "DType":
                    dtype = value
                elif key == "NOffsetBits":
                    n_offset_bits = int(value)
                elif key == "NSizeBits":
                    n_size_bits = int(value)
                elif key == "XExtent":
                    x_extent = int(value)
                elif key == "YExtent":
                    y_extent = int(value)
                elif key == "ZExtent":
                    z_extent = int(value)
                elif key == "BlockFilenames":
                    block_filenames = value
                elif key == "XStride":
                    x_stride = int(value)
                elif key == "YStride":
                    y_stride = int(value)
                elif key == "ZStride":
                    z_stride = int(value)
                elif key == "Compression":
                    compression = value
                elif key == "CompressionLvl":
                    compression_level = int(value)
                elif key == "Version":
                    version = value
                else:
                    application_metadata[key] = value
            return Directory(x_extent, y_extent, z_extent,
                             dtype, directory_filename,
                             x_block_size=x_block_size,
                             y_block_size=y_block_size,
                             z_block_size=z_block_size,
                             block_filenames=block_filenames,
                             x_stride=x_stride,
                             y_stride=y_stride,
                             z_stride=z_stride,
                             n_offset_bits=n_offset_bits,
                             n_size_bits=n_size_bits,
                             directory_offset=dir_offset,
                             compression=compression,
                             compression_level=compression_level,
                             metadata = application_metadata)

    def create(self):
        """
        Create a BlockFS filesystem from the initialization params.

        """
        metadata = self.metadata.copy()
        metadata["XBlockSize"] = self.x_block_size
        metadata["YBlockSize"] = self.y_block_size
        metadata["ZBlockSize"] = self.z_block_size
        metadata["DType"] = self.dtype.name
        metadata["NOffsetBits"] = self.n_offset_bits
        metadata["NSizeBits"] = self.n_size_bits
        metadata["XExtent"] = self.x_extent
        metadata["YExtent"] = self.y_extent
        metadata["ZExtent"] = self.z_extent
        metadata["BlockFilenames"] = self.block_filenames
        metadata["XStride"] = self.x_stride
        metadata["YStride"] = self.y_stride
        metadata["ZStride"] = self.z_stride
        metadata["Compression"] = self.compression.name
        metadata["CompressionLvl"] = self.compression_level
        metadata["Version"] = Directory.CURRENT_VERSION
        json_md = json.dumps(metadata).encode("UTF-8")
        self.directory_offset = len(Directory.HEADER) + 8 + len(json_md)
        with open(self.directory_filename, "wb") as fd:
            fd.write(Directory.HEADER)
            fd.write(np.array([len(json_md), self.directory_offset],
                              np.uint32).data)
            fd.write(json_md)

    def directory_writer_process(self):
        with open(self.directory_filename, "r+b") as fd:
            while True:
                msg = self.upqueue.get()
                if msg is None:
                    return
                directory_offset, offset, size = msg
                file_offset = self.directory_offset + \
                              directory_offset * self.directory_entry_size
                a = np.zeros(self.directory_entry_size, np.uint8)
                self.encode_directory_entry(a, offset, size)
                fd.seek(file_offset, os.SEEK_SET)
                fd.write(a.data)

    def get_block_size(self, x, y, z):
        return (min(self.z_extent - z, self.z_block_size),
                min(self.y_extent - y, self.y_block_size),
                min(self.x_extent - x, self.x_block_size))

    def write_block(self, block:np.ndarray, x:int, y:int, z:int):
        """
        Write a block to the filesystem

        :param block: a block of the appropriate size. At boundaries, please
        send a block that does not extend past the boundary.
        :param x: x coordinate of the block in pixels
        :param y: y coordinate of the block in pixels
        :param z: z coordinate of the block in pixels
        """
        expected_block_size = self.get_block_size(x, y, z)
        assert(tuple(block.shape) == expected_block_size)

        if self.writers is None:
            self.start_writer_processes()
        offset = self.offsetof(x, y, z)
        idx = offset % len(self.writers)
        self.writers[idx].write(block.astype(self.dtype), offset)

    def read_block(self, x, y, z):
        offset = self.offsetof(x, y, z)
        shape = self.get_block_size(x, y, z)
        idx = offset % len(self.block_filenames)
        directory_offset = self.directory_offset + \
                           offset * self.directory_entry_size
        if os.stat(self.directory_filename).st_size <\
            directory_offset + self.directory_entry_size:
            return np.zeros(shape, self.dtype)
        m = np.memmap(self.directory_filename, mode="r",
                      offset=directory_offset,
                      shape=(self.directory_entry_size,))
        offset, size = self.decode_directory_entry(m)
        if size == 0:
            return np.zeros(shape, self.dtype)
        with open(self.block_filenames[idx], "rb") as fd:
            fd.seek(offset)
            uncompressed = fd.read(size)
            blosc = Blosc(self.compression, self.compression_level)
        data = blosc.decode(uncompressed)
        return np.frombuffer(data, self.dtype).reshape(shape)
