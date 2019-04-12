import multiprocessing
import numpy as np
from numcodecs import Blosc
import os
import time
import logging
import subprocess

logger = logging.getLogger("blockfs.writer")

class WriterMessage:

    def __init__(self, a:np.ndarray, directory_offset:int):
        """
        Instantiate by capturing a message in a format that can be passed
        through shared memory to the writer

        :param a: a numpy array to be passed
        :param directory_offset: the directory offset where the position
        and count should be stored.
        """
        logger.debug("Creating writer message")
        self.a = a
        self.directory_offset = directory_offset

    def get(self):
        logger.debug("Getting writer message array")
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
    pid = os.getpid()
    logger.debug("%d: Starting block writer process for %s" % (pid, path))
    with open(path, "r+b") as fd:
        fd.seek(0, os.SEEK_END)
        blosc = Blosc(cname=compression, clevel=compression_level)
        while True:
            try:
                msg = q_in.get()
                logger.debug("%d: Got message from queue" % pid)
            except IOError:
                logger.exception("%d: Queue failed with I/O error" % pid)
                break
            if msg is None:
                logger.debug("%d: Got end-of-process message" % pid)
                break
            position = fd.tell()
            logger.debug("%d: Position = %d" % (pid, position))
            a = msg.get()
            block = blosc.encode(a)
            count = len(block)
            logger.debug("%d: Writing block of length %d" % (pid, count))
            fd.write(block)
            q_out.put((msg.directory_offset, position, count))
            q_in.task_done()
            logger.debug("%d: Task done: %d" % (pid, position))
    q_in.task_done()
    logger.info("Exiting process. PID=%d" % os.getpid())

class BlockWriter:

    def __init__(self, path:str, q_out:multiprocessing.JoinableQueue,
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
        logger.debug("Initializing block writer for path %s" % path)
        self.q_in = multiprocessing.JoinableQueue(maxsize=queue_depth)
        self.q_out = q_out
        self.process = multiprocessing.Process(
            target = block_writer_process,
            args=(path, compression, compression_level, self.q_in, self.q_out)
        )
        self.process.start()
        logger.debug("Block writer initialized: pid=%d" % self.process.pid)
        self.started = True
        self.stopped = False
        self.closed = False

    def stop(self):
        """Stop the writer process by sending it a message"""
        if self.stopped:
            return
        logger.debug("Stopping block writer: %d" % self.process.pid)
        self.q_in.put(None)
        self.stopped = True

    def close(self):
        """
        On closing, join to the block writer process.

        """
        if self.closed:
            return
        self.stop()
        logger.debug("Closing block writer: %d" % self.process.pid)
        while True:
            try:
                self.process.join(1)
                break
            except:
                # Desperation!!!
                logger.warn(
                    "In desperation, we send SIGINT to %d " % self.process.pid)
                subprocess.call(["kill", "-3", str(self.process.pid)])
        logger.debug("Block writer closed: %d" % self.process.pid)
        self.closed = True

    def write(self, a:np.ndarray, directory_offset:int):
        """
        Write a block to the blockfs.

        :param a: The array to be written
        :param directory_offset: this directory offset will be sent to the
        output queue once the block has been written
        """
        logger.debug("Sending block to queue. Directory offset = %d" %
                     directory_offset)
        msg = WriterMessage(a, directory_offset)
        self.q_in.put(msg)