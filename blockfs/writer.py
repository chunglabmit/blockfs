import multiprocessing
import numpy as np
from numcodecs import Blosc
import os
import time
import logging
import subprocess

logger = logging.getLogger()

EOT = "End of transmission"

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
    logger.info("%d: Starting block writer process for %s" % (pid, path))
    with open(path, "r+b") as fd:
        fd.seek(0, os.SEEK_END)
        position = fd.tell()
        blosc = Blosc(cname=compression, clevel=compression_level)
        while True:
            try:
                msg = q_in.get()
                logger.debug("%d: Got message from queue" % pid)
            except IOError:
                logger.exception("%d: Queue failed with I/O error" % pid)
                break
            if msg == EOT:
                logger.info("%d: Got end-of-process message" % pid)
                break
            logger.debug("%d: Position = %d" % (pid, position))
            a = msg.get()
            block = blosc.encode(a)
            count = len(block)
            logger.debug("%d: Writing block of length %d" % (pid, count))
            fd.write(block)
            q_out.put((msg.directory_offset, position, count))
            position += len(block)
            logger.debug("%d: Task done: %d" % (pid, position))
    logger.info("Making sure q_out is empty: %d" % pid)
    while not q_out.empty():
        time.sleep(.25)
    logger.info("Exiting process. PID=%d" % os.getpid())

class BlockWriter:

    def __init__(self, path:str, q_out:multiprocessing.Queue,
                 q_in:multiprocessing.Queue,
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
        logger.info("Initializing block writer for path %s" % path)
        self.q_in = q_in
        self.q_out = q_out
        self.process = multiprocessing.Process(
            target = block_writer_process,
            args=(path, compression, compression_level, self.q_in, self.q_out)
        )
        self.started = False
        self.stopped = False
        self.closed = False
        self.pid = 0

    def start(self):
        self.process.start()
        self.pid = self.process.pid
        logger.info("Block writer started: pid=%d" % self.pid)
        self.started = True

    def stop(self):
        """Stop the writer process by sending it a message"""
        if self.stopped:
            return
        while not self.q_in.empty():
            logger.info("Waiting for input queue to empty: %d" % self.pid)
            time.sleep(.25)
        while not self.q_out.empty():
            logger.info("Waiting for output queue to empty: %d" % self.pid)
            time.sleep(.25)
        logger.info("Stopping block writer: %d" % self.pid)
        self.q_in.put(EOT)
        self.stopped = True
        logger.info("Block writer stopped: %d" % self.pid)

    def close(self):
        """
        On closing, join to the block writer process.

        """
        if self.closed:
            return
        self.stop()
        logger.info("Closing block writer: %d" % self.pid)
        if self.started:
            self.process.join()
        logger.info("Block writer closed: %d" % self.pid)
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