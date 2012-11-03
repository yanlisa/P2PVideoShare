from streamer import StreamFTP
from threadclient import ThreadClient
from server import *
from pyftpdlib import ftpserver
import Queue
import random

class Cache(object):
    """
    Manages the cache as a whole. Has 3 main functions:
    -Opens up an FTP mini-server (CacheServer) in one thread.
    -Opens up a connection to the actual server (ServerDownloader) in a thread.
    -Every <timestep> check to see if cache is properly serving user-needs.
    If not, use ServerDownloader to request different chunks from the server.
    """

    def __init__(self, address, path, packet_size=2504):
        """Make the FTP server instance and the Cache Downloader instance.
        Obtain the queue of data from the FTP server."""

        self.authorizer = ftpserver.DummyAuthorizer()
        # allow anonymous login.
        self.authorizer.add_anonymous(path, perm='elr')
        handler = CacheHandler
        handler.authorizer = authorizer
        handler.masquerade_address = '107.21.135.254'
        handler.passive_ports = range(60000, 65535)

        handler.set_packet_size(packet_size)
        self.mini_server = ThreadServer(address, handler)

    def start_cache(self):
        """Start the FTP server and the CacheDownloader instance.
        Every <timestamp>, obtain the recorded data from the FTP server queue
        and ask the server for additional chunks if needed."""
        self.mini_server.start()
        pass

new_proto_cmds = ftpserver.proto_cmds
new_proto_cmds['CNKS'] = dict(perm='l', auth=True, arg=None,
                              help='Syntax: CNKS (list available chunk nums).')

class CacheHandler(StreamHandler):
    """
    The mini-server handler that serves users on this address.

    The mini-server only stores a particular set of chunks per frame,
    and that set will be what it sends to the user per frame requested.

    A new set of chunks is modified based on the transaction records, and this
    is done in a separate thread.
    """
    def __init__(self, conn, server, wait_time=1):
        StreamHandler.__init__(self, conn, server, wait_time)
        self.chunks = []
        self.transaction_record = Queue.Queue()
        while len(self.chunks) < 15:
            x = random.randint(0, 40)
            if not x in self.chunks:
                self.chunks.append(x)
        self.proto_cmds = new_proto_cmds

    def get_chunks(self):
        return self.chunks

    def get_transactions(self):
        return self.transaction_record

    def ftp_CNKS(self):
        data = str(self.chunks)
        self.push_dtp_data(data, isproducer=False, cmd="CNKS")

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the cache to the
        client).

        Accepts filestrings of the form:
            file-<filename>.<ext>&<framenum>

        If the file has an integer extension, assume it is asking for a
        file frame. cd into the correct directory and transmit all chunks
        the server has for that frame.
        """
        framenum = (file.split('&'))[-1]
        if framenum.isdigit():
            try:
                # filename should be prefixed by "file-" in order to be valid.
                # frame number is expected to exist for this cache.
                filename=(((file.split('.'))[0]).split('file-'))[1]
                chunksdir = 'chunks-' + filename
                framedir = filename + '.' + framenum + '.dir'
                path = self.movies_path + '/' + chunksdir + '/' + framedir
                # get chunks list and open up all files
                files = self.get_chunk_files(path)
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)

            producer = self.chunkproducer(files, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
            return

class ServerDownloader(ThreadClient, threading.Thread):
    """
    Requests new chunks from the server. Is always connected to the server.

    Since the chunk size is always fixed, fix the expected packet_size.
    """
    def __init__(self, address, packet_size):
        threading.Thread.__init__(self)
        StreamFTP.__init__(self, address, packet_size)
        self.client.set_callback(self.chunkcallback)

    def put_instruction(self, cmd_string):
        """Something or other"""

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

if __name == "__main__":
    address = ("10.0.1.2", 24) 
    path = "/Users/Lisa/Research"

    path = "/home/ec2-user/"
    address = ("10.29.147.60", 21)

    print "Arguments:", sys.argv
    packet_size = 2504
    if len(sys.argv) > 1:
        packet_size = int(sys.argv[1])

    cache = Cache(address, path, packet_size)
    cache.start_cache()
