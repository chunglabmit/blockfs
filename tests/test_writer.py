import unittest
import numpy as np
import multiprocessing
import blockfs.writer as w
import tempfile
from numcodecs import Blosc

class TestWriter(unittest.TestCase):
    def test_01_writer_message(self):
        a = np.random.randint(0, np.iinfo(np.uint16).max, (4, 5, 6))
        msg = w.WriterMessage(a, 1234)
        self.assertEqual(msg.directory_offset, 1234)
        np.testing.assert_array_equal(msg.get(), a)

    def test_02_writer_open_close(self):
        with tempfile.NamedTemporaryFile() as tf:
            q_in = multiprocessing.Queue()
            q_out = multiprocessing.Queue()
            writer = w.BlockWriter(tf.name, q_out, q_in, "zstd", 5)
            writer.start()
            writer.close()

    def test_03_writer_send(self):
        with tempfile.NamedTemporaryFile() as tf:
            q_in = multiprocessing.Queue()
            q_out = multiprocessing.Queue()
            a = np.random.randint(0, np.iinfo(np.uint16).max, (4, 5, 6))
            writer = w.BlockWriter(tf.name, q_out, q_in, "zstd", 5)
            writer.start()
            writer.write(a, 1234)
            directory_offset, position, size = q_out.get()
            writer.close()
            self.assertEqual(directory_offset, 1234)
            self.assertEqual(position, 0)
            block = tf.file.read()
            self.assertEqual(len(block), size)
            a_out = np.frombuffer(Blosc("zstr", 5).decode(block),
                                  a.dtype).reshape(a.shape)
            np.testing.assert_array_equal(a, a_out)



if __name__ == '__main__':
    unittest.main()
