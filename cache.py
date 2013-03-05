from streamer import StreamFTP
import threadclient
from server import *
from pyftpdlib import ftpserver
import Queue
import random
import csv
import time
import threading

# Debugging MSG
DEBUGGING_MSG = True
# Cache Configuration
cache_config_file = '../../config/cache_config.csv'

MAX_CONNS = 10

# IP Table
class ThreadStreamFTPServer(StreamFTPServer, threading.Thread):
    """
        A threaded server. Requires a Handler.
    """
    def __init__(self, address, handler, spec_rate=0):
        StreamFTPServer.__init__(self, address, handler, spec_rate)
        threading.Thread.__init__(self)

    def run(self):
        self.serve_forever()

    def get_conns(self):
        return self.conns

    def get_handlers(self):
        return self.handlers

class Cache(object):
    """
    Manages the cache as a whole. Has 3 main functions:
    -Opens up an FTP mini-server (CacheServer) in one thread.
    -Opens up a connection to the actual server (ServerDownloader) in a thread.
    -Every <timestep> check to see if cache is properly serving user-needs.
    If not, use ServerDownloader to request different chunks from the server.
    """

    def __init__(self, cache_config):
        """Make the FTP server instance and the Cache Downloader instance.
        Obtain the queue of data from the FTP server."""

        # Parse cache configuration
        # [ cache_id,
        #   private_ip, port #,
        #   masq_ip,
        #   stream_rate,
        #   num_of_chunks_cache_stores ]

        cache_id = int(cache_config[0])
        address = (cache_config[1], int(cache_config[2]))
        print '[cache.py] Address : ', address
        masq_address = cache_config[3]
        stream_rate = int(cache_config[4])
        num_of_chunks_cache_stores = int(cache_config[5])

        chunks = []
        while len(chunks) < num_of_chunks_cache_stores:
            x = random.randint(0, 40)
            if not x in chunks:
                chunks.append(x)

        self.authorizer = ftpserver.DummyAuthorizer()
        # allow anonymous login.
        self.authorizer.add_anonymous(path, perm='elr')
        handler = CacheHandler
        handler.authorizer = self.authorizer
        # handler.masquerade_address = '107.21.135.254'
        handler.passive_ports = range(60000, 65535)

        self.rates = [0]*MAX_CONNS # in chunks per frame
        self.chunks = [chunks]*MAX_CONNS
        self.binary_g = [0]*MAX_CONNS # binary: provided chunks sufficient for conn

        handler.chunks = self.chunks # static
        handler.rates = self.rates # static
        handler.binary_g = self.binary_g # static

        self.mini_server = ThreadStreamFTPServer(address, handler, stream_rate)
        print "Cache streaming rate set to ", self.mini_server.stream_rate
        handler.set_movies_path(path)

        if (DEBUGGING_MSG):
            print "Chunks available on this cache:", chunks

    def start_cache(self):
        """Start the FTP server and the CacheDownloader instance.
        Every <timestamp>, obtain the recorded data from the FTP server queue
        and ask the server for additional chunks if needed."""
        print 'Trying to run a cache...'
        self.mini_server.start()
        print 'Cache is running...'

    def get_conns(self):
        return self.mini_server.get_conns()

    def set_conns(self, index, spec_rate):
        self.mini_server.get_handlers()[index].set_stream_rate(spec_rate)

    def get_g(self, index):
        return self.mini_server.get_g()[index]

###### HANDLER FOR EACH CONNECTION TO THIS CACHE######

new_proto_cmds = proto_cmds # from server.py
new_proto_cmds['CNKS'] = dict(perm='l', auth=True, arg=None,
                              help='Syntax: CNKS (list available chunk nums).')

class CacheHandler(StreamHandler):
    """
    The mini-server handler that serves users on this address.

    The mini-server only stores a particular set of chunks per frame,
    and that set will be what it sends to the user per frame requested.
    """
    chunks = []
    rates = []
    stream_rate = 10*1024 # Default is 10 Kbps
    def __init__(self, conn, server, index=0, spec_rate=0):
        self.proto_cmds = new_proto_cmds
        super(CacheHandler, self).__init__(conn, server, index, spec_rate)

    def set_rates(self, new_rate):
        """
        Adjusts the list of rate that this cache holds across all frames.
        """
        CacheHandler.rates[self.index] = new_rate

    def set_chunks(self, new_chunks):
        """
        Adjusts the set of chunks that this cache holds across all frames.
        """
        CacheHandler.chunks[self.index] = new_chunks 

    def set_binary_g(self, new_g):
        """
        Adjusts the binary value of the user's satisfaction with this connection.
        """
        CacheHandler.binary_g[self.index] = new_g

    def ftp_CNKS(self, line):
        """
        FTP command: Returns this cache's chunk number set.
        """
        print "index:", self.index
        data = str(CacheHandler.chunks[self.index])
        data = data + '&' + str(CacheHandler.rates[self.index])
        self.push_dtp_data(data, isproducer=False, cmd="CNKS")

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).

        Accepts filestrings of the form:
            chunk-<filename>.<ext>&<framenum>/<chunknum>
            file-<filename>
        """
        if DEBUGGING_MSG:
            print file
        parsedform = threadclient.parse_chunks(file)
        if parsedform:
            filename, framenum, binary_g, chunks = parsedform
            try:
                # filename should be prefixed by "file-" in order to be valid.
                # frame number is expected to exist for this cache.
                chunksdir = 'chunks-' + filename
                framedir = filename + '.' + framenum + '.dir'
                path = self.movies_path + '/' + chunksdir + '/' + framedir
                # get chunks list and open up all files
                files = self.get_chunk_files(path, chunks)
    
                if DEBUGGING_MSG:
                    print "chunks requested:", chunks
                    print 'chunksdir', chunksdir
                    print 'framedir', framedir
                    print 'path', path
                    print 'binary_g', binary_g
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)

            CacheHandler.binary_g[self.index] = binary_g 
            producer = self.chunkproducer(files, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
            return

    def get_chunk_files(self, path, chunks=None):
        """For the specified path, open up all files for reading. and return
        an array of file objects opened for read.

        Only return the file objects specified by chunk numbers argument."""
        files = Queue.Queue()
        if not chunks:
            return files 
        iterator = self.run_as_current_user(self.fs.get_list_dir, path)
        for x in xrange(self.max_chunks):
            liststr = iterator.next()
            filename = ((liststr.split(' ')[-1]).split('\r'))[0]
            chunk_num = (filename.split('_')[0]).split('.')[-1]
            if chunk_num.isdigit() and int(chunk_num) in chunks:
                filepath = path + '/' + filename
                fd = self.run_as_current_user(self.fs.open, filepath, 'rb')
                files.put(fd)
                if (DEBUGGING_MSG):
                    print "Sending chunk", filename

        return files

class ServerDownloader(threadclient.ThreadClient, threading.Thread):
    """
    Requests new chunks from the server. Is always connected to the server.

    Since the chunk size is always fixed, fix the expected packet_size.
    """
    def __init__(self, address, packet_size):
        threading.Thread.__init__(self)
        StreamFTP.__init__(self, address, chunk_size=packet_size)
        self.client.set_callback(self.chunkcallback)

    def put_instruction(self, cmd_string):
        """Something or other"""
        pass

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

def start_control(cache):
    while not cache.get_conns():
        pass
    conn_index = 0
    single_conn = cache.get_conns()[conn_index]
    print "Conn accepted with address", single_conn
    for i in range(5):
        cache.rates[conn_index] = i
        print 'Set rate as ', i
        time.sleep(10)
    # spec_rate = 100000
    # cache.set_conns(conn_index, spec_rate)

def load_cache_config(cache_id):
    f = open(cache_config_file)
    fs = csv.reader(f, delimiter = ' ')
    for row in fs:
        if int(row[0]) == cache_id:
            if (DEBUGGING_MSG): print '[cache.py] Cache configuration : ', row
            return row
    # If not found
    return None

if __name__ == "__main__":
    if len(sys.argv) == 2:
        config = load_cache_config(int(sys.argv[1])) # Look up configuration of the given cache ID
        if config == None:
            print '[cache.py] cache_id not found'
            sys.exit()
    else:
        print '[cache.py] cache.py needs an argument "cache_id"'
        sys.exit()

    cache = Cache(config)
    cache.start_cache()
    start_control(cache)
