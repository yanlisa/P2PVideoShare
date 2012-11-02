from streamer import StreamFTP
from threadclient import ThreadClient
from server import *
import random

class Cache(object):
    """
    Manages the cache as a whole. Has 3 main functions:
    -Opens up an FTP mini-server (CacheFTP) in one thread.
    -Opens up a connection to the actual server (ServerDownloader) in a thread.
    -Every <timestep> check to see if cache is properly serving user-needs.
    If not, use ServerDownloader to request different chunks from the server.
    """

    def __init__(self, address):
        """Make the FTP server instance and the Cache Downloader instance.
        Obtain the queue of data from the FTP server."""
        pass

    def start_cache(self):
        """Start the FTP server and the CacheDownloader instance.
        Every <timestamp>, obtain the recorded data from the FTP server queue
        and ask the server for additional chunks if needed."""
        pass

class CacheFTP(StreamHandler):
    """
    The mini-server that serves users on this address.

    This mini-server only stores a particular set of chunks per frame,
    and that set will be what it sends to the user per frame requested.

    A new set of chunks is modified based on the 
    """
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

class ServerDownloader(ThreadClient, object):
    """
    Requests new chunks from the server. Is always connected to the server.

    Since the chunk size is always fixed, fix the expected packet_size.
    """
    def __init__(self, address, packet_size):
        StreamFTP.__init__(self, address, packet_size)
        self.client.set_callback(self.chunkcallback)

    def put_instruction(self, cmd_string):
        """Something or other"""
        if 

    def chunkcallback(self, chunk_size, fname):
        # directory name by convention is filename itself.
        def helper(data):
            file_to_write = open(fname, 'a+b')
            datastring = data + chunk_num_and_data[1]
            curr_bytes = sys.getsizeof(datastring)
            outputStr = "%s: Received %d bytes. Current Total: %d bytes.\n" % \
                (filestr, sys.getsizeof(data), curr_bytes)
            sys.stdout.write(outputStr)
            sys.stdout.flush()
            outputStr = "Writing %d bytes to %s.\n" %  (curr_bytes, filestr)
            sys.stdout.write(outputStr)
            sys.stdout.flush()
            file_to_write.write(datastring)
            file_to_write.close()
    
        return helper
