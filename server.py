import sys, errno
from pyftpdlib import ftpserver
import Queue, time, re
import threading

class ThreadServer(ftpserver.FTPServer, threading.Thread):
    """
        A threaded server. Requires a Handler.
    """
    def __init__(self, address, handler):
        ftpserver.FTPServer.__init__(self, address, handler)
        threading.Thread.__init__(self)
    
    def run(self):
        self.serve_forever()

class StreamHandler(ftpserver.FTPHandler):
    """The general handler for an FTP Server in this network.
    CacheHandler, a specific Handler to use for Caches, inherits from this one.

    Has two different responses for ftp_RETR:
    -If type is of the form 'chunk-<filename>.<int>', send all 
    """
    packet_size = 100 # default (in bytes)
    max_chunks = 40
    movies_path='/home/ec2-user/movies'

    def __init__(self, conn, server):
        (super(StreamHandler, self)).__init__(conn, server)
        self._close_connection = False
        self.dtp_handler = DTPPacketHandler
        self.dtp_handler.set_buffer_size(self.packet_size)
        self.producer = ftpserver.FileProducer
        self.chunkproducer = FileChunkProducer
        self.chunkproducer.set_buffer_size(self.packet_size)

    def get_chunks(self):
        return range(1, 40)

    @staticmethod
    def set_packet_size(packet_size):
        StreamHandler.packet_size = packet_size

    @staticmethod
    def set_movies_path(path):
        StreamHandler.movies_path = path

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).

        Accepts filestrings of the form:
            chunk-<filename>.<ext>&<framenum>/<chunknum>
            file-<filename>
        """
        chunk_prefix = (file.split('-'))[0]
        if chunk_prefix == "chunk":
            chunknum = (file.split('/'))[-1]
            framenum = (file.split('.')[-1]).split('/')[0]
            if chunknum.isdigit() and framenum.isdigit():
                try:
                    filename = ((file.split('.'))[0]).split('chunk-')[1]
                    chunksdir = 'chunks-' + filename
                    framedir = filename + '.' + framenum + '.dir'
                    path = StreamHandler.movies_path + '/' + chunksdir + '/' + framedir
                    filepath = path + '/' # INCOMPLETE. Need to decide on file format.
                    # get chunks list and open up all files
                    files = self.get_chunk_files(path)
                except OSError, err:
                    why = ftpserver._strerror(err)
                    self.respond('550 %s.' % why)

                producer = self.chunkproducer(files, self._current_type)
                self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
                return


        if file.find(StreamHandler.movies_path) == 0:
            file = file[15:]
            print 'request: %s' % file

        rest_pos = self._restart_position
        self._restart_position = 0
        try:
            iterator = self.run_as_current_user(self.fs.get_list_dir,
                                                StreamHandler.movies_path)
            files = (MovieLister(iterator)).more() + '\n'
            files_list = files.split('\r\n')[:-1]
            regex = re.compile(file + '.*')
            found_file = None
            for file_entry in files_list:
                found_file = regex.match(file_entry)
                if found_file:
                    break
            if found_file == None:
                raise IOError('file number not found')
            found_file = found_file.group()
            print('found the file: file-%s' % found_file[len(file) + 2:])
            found_file = StreamHandler.movies_path + '/file-' + found_file[len(file) + 2 :]
            fd = self.run_as_current_user(self.fs.open, found_file, 'rb')
        except IOError, err:
            try:
                fd = self.run_as_current_user(self.fs.open, StreamHandler.movies_path + '/' + file, 'rb')
            except IOError, err:
                #why = _strerror(err)
                why = str(err)
                self.respond('550 %s.' % why)
                return

        if rest_pos:
            # Make sure that the requested offset is valid (within the
            # size of the file being resumed).
            # According to RFC-1123 a 554 reply may result in case that
            # the existing file cannot be repositioned as specified in
            # the REST.
            ok = 0
            try:
                if rest_pos > self.fs.getsize(file):
                    raise ValueError
                fd.seek(rest_pos)
                ok = 1
            except ValueError:
                why = "Invalid REST parameter"
            except IOError, err:
                why = _strerror(err)
            if not ok:
                self.respond('554 %s' % why)
                return
        producer = self.producer(fd, self._current_type)
        self.push_dtp_data(producer, isproducer=True, file=fd, cmd="RETR")

    def get_chunk_files(self, path):
        """For the specified path, open up all files for reading. and return
        an array of file objects opened for read."""
        iterator = self.run_as_current_user(self.fs.get_list_dir, path)
        files = Queue.Queue()
        for x in xrange(self.max_chunks):
            try:
                liststr = iterator.next()
                filename = ((liststr.split(' ')[-1]).split('\r'))[0]
                filepath = path + '/' + filename
                fd = self.run_as_current_user(self.fs.open, filepath, 'rb')
                files.put(fd)
            except StopIteration, err:
                why = _strerror(err)
                self.respond('544 %s' %why)
                break
        return files 

    def ftp_LIST(self, path):
        """Return a list of files in the specified directory to the
        client.
        """
        # - If no argument, fall back on cwd as default.
        # - Some older FTP clients erroneously issue /bin/ls-like LIST
        #   formats in which case we fall back on cwd as default.
        try:
            iterator = self.run_as_current_user(self.fs.get_list_dir, StreamHandler.movies_path)
        except OSError, err:
            why = ftpserver._strerror(err)
            self.respond('550 %s.' % why)
        else:
            producer = MovieLister(iterator)
            self.push_dtp_data(producer, isproducer=True, cmd="LIST")

    def _on_dtp_connection(self):
        """For debugging purposes."""
        print "Calling _on_dtp_connection."
        return super(StreamHandler, self)._on_dtp_connection()

class DTPPacketHandler(ftpserver.DTPHandler):
    def __init__(self, sock_obj, cmd_channel, out_buffer_size=65536):
        super(DTPPacketHandler, self).__init__(sock_obj, cmd_channel)
        DTPPacketHandler.ac_out_buffer_size = out_buffer_size

    @staticmethod
    def set_buffer_size(out_buffer_size):
        DTPPacketHandler.ac_out_buffer_size = out_buffer_size

class FileStreamProducer(ftpserver.FileProducer):
    """
    This class is not currently in use, but we will keep it here.
    
    Wraps around FileProducer such that reading is limited by
    packet_size.
    Wait 0.1 s before calling more().
    Default buffer_size is 65536 as specified in FileProducer.
    """
    buffer_size = 65535
    wait_time = 0.1
    def __init__(self, file, type, buff=0):
        if buff:
            self.buffer_size = buff
        super(FileStreamProducer, self).__init__(file, type)

    @staticmethod
    def set_buffer_size(buffer_size):
        FileStreamProducer.buffer_size = buffer_size

    @staticmethod
    def set_wait_time(wait_time):
        FileStreamProducer.wait_time = wait_time

    def more(self):
        time.sleep(self.wait_time)
        data = super(FileStreamProducer, self).more()
        outputStr = "Size of packet to send: %d\n" % sys.getsizeof(data)
        sys.stdout.write(outputStr)
        sys.stdout.flush()
        return data

class FileChunkProducer(FileStreamProducer):
    """Takes a queue of file chunk objects and attempts to send
    one with each call to self.more().
    
    If the network is limited, just send as much of each file chunk object
    as possible at a time, then send the remaining part of that file chunk
    on the next iteration and close the file chunk object. On the
    following iteration, send the next file chunk.
    """
    def __init__(self, filequeue, type):
        self.file_queue = filequeue
        self.curr_producer = None
        self.type = type
        if not self.file_queue.empty():
            self.curr_producer = FileStreamProducer( \
                self.file_queue.get(), self.type, self.buffer_size)

    @staticmethod
    def set_buffer_size(buffer_size):
        FileChunkProducer.buffer_size = buffer_size

    def more(self):
        if self.curr_producer:
            data = self.curr_producer.more()
            if not data:
                if not self.file_queue.empty():
                    self.curr_producer = FileStreamProducer( \
                        self.file_queue.get(), self.type, self.buffer_size)
                    data = self.curr_producer.more()
            return data
        return None

class MovieLister(ftpserver.BufferedIteratorProducer):
    def __init__(self, iterator):
        super(MovieLister, self).__init__(iterator)

    def more(self):
        """Attempt a chunk of data from iterator by calling
        its next() method different times.  Also, number each
        file so the user can select the file by number, and
        simplify the output.
        """
        buffer = []
        i = 1
        for x in xrange(self.loops):
            try:
                next = self.iterator.next()
                file_format = next.split('file-')
                if len(file_format) > 1:
                    buffer.append(str(i) + ': ' + file_format[-1])
                    i += 1
            except StopIteration:
                break
        return ''.join(buffer)[:-1]

def main_no_stream(user_params):
    user = "user" + "1"
    pw = "1"

    authorizer = ftpserver.DummyAuthorizer()
    authorizer.add_user(user, pw, "/home/ec2-user", perm='elr')
    # allow anonymous login.
    authorizer.add_anonymous("/home/ec2-user", perm='elr')

    handler = ftpserver.FTPHandler
    handler.authorizer = authorizer
    handler.masquerade_address = '107.21.135.254'
    handler.passive_ports = range(60000, 65535)
    address = ("10.29.147.60", 21)
    ftpd = ftpserver.FTPServer(address, handler)
    ftpd.serve_forever()

def main():
    """Parameters:
        No parameters: run with defaults (assume on ec2server)
        arg1: Packet_size
        arg2: IP address 
        arg3: path
    """
    path = "/home/ec2-user/"
    address = ("10.29.147.60", 21)

    print "Arguments:", sys.argv
    if len(sys.argv) > 1:
        packet_size = int(sys.argv[1])
        StreamHandler.set_packet_size(packet_size)
        print "StreamHandler now has size ", StreamHandler.packet_size
    if len(sys.argv) == 4:
        address = (sys.argv[2], 21)
        path = sys.argv[3]

    authorizer = ftpserver.DummyAuthorizer()
    # allow anonymous login.
    authorizer.add_anonymous(path, perm='elr')
    handler = StreamHandler
    handler.authorizer = authorizer
    handler.masquerade_address = '107.21.135.254'
    handler.passive_ports = range(60000, 65535)
    ftpd = ftpserver.FTPServer(address, handler)
    ftpd.serve_forever()

if __name__ == "__main__":
    main()
