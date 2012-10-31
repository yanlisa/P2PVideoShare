from server import *
import random

class cacheManager(StreamHandler):

    def __init__(self, conn, server, wait_time=1):
        (super(StreamHandler, self)).__init__(conn, server)
        self._close_connection = False
        self._wait_time = wait_time
        self.dtp_handler = DTPPacketHandler
        self.dtp_handler.set_buffer_size(self.packet_size)
        self.producer = FileStreamProducer
        self.producer.set_buffer_size(self.packet_size)
        self.chunkproducer = FileChunkProducer
        self.chunkproducer.set_buffer_size(self.packet_size)
        self.chunks = []
        while len(self.chunks) < 15:
            x = random.randint(0, 40)
            if not x in self.chunks:
                self.chunks.append(x)

    def get_chunks(self):
        return self.chunks
