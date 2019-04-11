import multiprocessing
import numpy as np
from numcodecs import Blosc
import os


class WriterMessage:

    def __init__(self, a:np.ndarray, directory_offset:int):
        """
        Instantiate by capturing a message in a format that can be passed
        through shared memory to the writer

        :param a: a numpy array to be passed
        :param directory_offset: the directory offset where the position
        and count should be stored.
        """
        self.a = a
        self.directory_offset = directory_offset

    def get(self):
        return self.a

def block_writer_process(
        path:str,
        compression:str,
        compression_level:int,
        q_in:multiprocessing.Queue,
        q_out:multiprocessing.Queue):
    """
    The process function for a writer process

    :param path: the path to the file that the writer writes to
    :param compression: the compression method used for Blosc
    :param compression_level: the compression level to be used
    :param q_in: WriterMessages come down this queue. The process ends when
                 this queue is closed and nothing remains to be read.
    :param q_out: We send the offset and size down this queue to indicate that
                  the message has been passed
    """
    with open(path, "rb+") as fd:
        fd.seek(0, os.SEEK_END)
        blosc = Blosc(cname=compression, clevel=compression_level)
        while True:
            try:
                msg = q_in.get()
            except IOError:
                return
            if msg is None:
                return
            position = fd.tell()
            a = msg.get()
            block = blosc.encode(a)
            count = len(block)
            fd.write(block)
            q_out.put((msg.directory_offset, position, count))


class BlockWriter:

    def __init__(self, path:str, q_out:multiprocessing.Queue,
                 compression:str, compression_level:int,
                 queue_depth:int = 10):
        """
        Initialize the block writer with the blockfs file's path and
        the queue which will get the write messages.

        :param path: path to the blockfs file
        :param q_out: the queue that the writer process should write to.
        :param compression: the BLOSC compression type
        :param compression_level: the compression level to use
        :param queue_depth: This limits the number of chunks that can be
        enqueued before the queue blocks. A large queue depth will eat up
        lots of memory.
        """
        self.q_in = multiprocessing.Queue(maxsize=queue_depth)
        self.q_out = q_out
        self.process = multiprocessing.Process(
            target = block_writer_process,
            args=(path, compression, compression_level, self.q_in, self.q_out)
        )
        self.process.start()

    def close(self):
        """
        On closing, join to the block writer process.

        """
        self.q_in.put(None)
        self.q_in.close()
        self.process.join()

    def write(self, a:np.ndarray, directory_offset:int):
        """
        Write a block to the blockfs.

        :param a: The array to be written
        :param directory_offset: this directory offset will be sent to the
        output queue once the block has been written
        """
        msg = WriterMessage(a, directory_offset)
        self.q_in.put(msg)