from streamer import StreamFTP
import threadclient
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

        self.chunks = []
        while len(self.chunks) < 15:
            x = random.randint(0, 40)
            if not x in self.chunks:
                self.chunks.append(x)

        self.authorizer = ftpserver.DummyAuthorizer()
        # allow anonymous login.
        self.authorizer.add_anonymous(path, perm='elr')
        handler = CacheHandler
        handler.authorizer = self.authorizer
        # handler.masquerade_address = '107.21.135.254'
        handler.passive_ports = range(60000, 65535)

        handler.set_packet_size(packet_size)
        handler.set_chunks(self.chunks)
        self.mini_server = ThreadServer(address, handler)
        handler.set_movies_path(path)

        if (True):
            print "Chunks available on this cache:", self.chunks

    def start_cache(self):
        """Start the FTP server and the CacheDownloader instance.
        Every <timestamp>, obtain the recorded data from the FTP server queue
        and ask the server for additional chunks if needed."""
        self.mini_server.start()

new_proto_cmds = proto_cmds # from server.py
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
    chunks = []
    def __init__(self, conn, server):
        StreamHandler.__init__(self, conn, server)
        self.transaction_record = Queue.Queue()
        self.proto_cmds = new_proto_cmds

    @staticmethod
    def set_chunks(chunks):
        """
        Adjusts the set of chunks that this cache holds across all frames.
        """
        CacheHandler.chunks = chunks

    def get_chunks(self):
        """
        Returns the set of chunks that this cache holds across all frames.
        """
        return CacheHandler.chunks

    def get_transactions(self):
        """
        HAS NOT BEEN IMPLEMENTED YET
        The record of client transactions on this cache.
        """
        return self.transaction_record

    def ftp_CNKS(self, line):
        """
        FTP command: Returns this cache's chunk number set.
        """
        data = str(CacheHandler.chunks)
        self.push_dtp_data(data, isproducer=False, cmd="CNKS")
        self.transaction_record.put(("CNKS", CacheHandler.chunks))

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the cache to the
        client).

        Accepts filestrings of the form:
            file-<filename>.<framenum>.<cnk1>%<cnk2>%<cnk3>

        If the file has an integer extension, assume it is asking for a
        file frame. cd into the correct directory and transmit all chunks
        the server has for that frame.
        """
        self.transaction_record.put(("RETR", file))
        parsedform = threadclient.parse_chunks(file)
        if parsedform:
            filename, framenum, chunks = parsedform
            print "chunks requested:", chunks
            try:
                # filename should be prefixed by "file-" in order to be valid.
                # frame number is expected to exist for this cache.
                chunksdir = 'chunks-' + filename
                framedir = filename + '.' + framenum + '.dir'
                path = self.movies_path + '/' + chunksdir + '/' + framedir
                # get chunks list and open up all files
                files = self.get_chunk_files(path, chunks)
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)

            producer = self.chunkproducer(files, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
            return
        why = "Invalid filename. Usage: RETR file-<filename>.<framenum>.<cnk1>%<cnk2>..."
        self.respond('554 %s.' % why)

    def get_chunk_files(self, path, chunks=None):
        """For the specified path, open up all files for reading. and return
        an array of file objects opened for read.

        Only return the file objects specified by chunk numbers argument."""
        if not chunks:
            chunks = self.chunks
        iterator = self.run_as_current_user(self.fs.get_list_dir, path)
        files = Queue.Queue()
        for x in xrange(self.max_chunks):
            #try:
            liststr = iterator.next()
            filename = ((liststr.split(' ')[-1]).split('\r'))[0]
            chunk_num = (filename.split('_')[0]).split('.')[-1]
            if chunk_num.isdigit() and int(chunk_num) in chunks:
                filepath = path + '/' + filename
                fd = self.run_as_current_user(self.fs.open, filepath, 'rb')
                files.put(fd)
                if (True):
                    print filename, chunk_num
                    print "Sending chunk_num", chunk_num
            #except StopIteration, err:
                #print x
                #why = ftpserver._strerror(err)
                #self.respond('544 %s' %why)
                #break

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

if __name__ == "__main__":
    address = ("10.10.66.187", 21) # local airbears
    # path = "/home/nick/Dropbox/Berkeley 2012-2013/Research/P2PVideoShare/"
    path = "/Users/Lisa/Research/"

    print "Arguments:", sys.argv
    packet_size = 5 * 1024 * 1024
    if len(sys.argv) > 1:
        packet_size = int(sys.argv[1])

    cache = Cache(address, path, packet_size)
    cache.start_cache()
