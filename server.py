import sys, errno
from pyftpdlib import ftpserver
import Queue, time, re
import threading
import threadclient

server_address = ("localhost", 61000)
path = "."

class ThreadServer(ftpserver.FTPServer, threading.Thread):
    """
        A threaded server. Requires a Handler.
    """
    def __init__(self, address, handler):
        ftpserver.FTPServer.__init__(self, address, handler)
        threading.Thread.__init__(self)

    def run(self):
        self.serve_forever()

# The FTP commands the server understands.
proto_cmds = ftpserver.proto_cmds
proto_cmds['VLEN'] = dict(perm='l', auth=True, arg=True,
                              help='Syntax: VLEN (video length: number of frames total).')
proto_cmds['CNKS'] = dict(perm='l', auth=True, arg=None,
                              help='Syntax: CNKS (list available chunk nums).')

class StreamHandler(ftpserver.FTPHandler):
    """The general handler for an FTP Server in this network.
    CacheHandler, a specific Handler to use for Caches, inherits from this one.

    Has two different responses for ftp_RETR:
    -If type is of the form 'chunk-<filename>.<int>', send all
    """
    stream_rate = 10*1024 # default (10 Kbps)
    max_chunks = 40
    movies_path = path

    def __init__(self, conn, server):
        (super(StreamHandler, self)).__init__(conn, server)
        self._close_connection = False
        self.dtp_handler = ftpserver.ThrottledDTPHandler
        self.producer = ftpserver.FileProducer
        self.dtp_handler.read_limit = self.stream_rate  # b/sec (ex 30Kbps = 30*1024)
        self.dtp_handler.write_limit = self.stream_rate # b/sec (ex 30Kbps = 30*1024)
        self.chunkproducer = FileChunkProducer
        self.proto_cmds = proto_cmds

    def get_chunks(self):
        return range(1, 40)

    @staticmethod
    def set_stream_rate(stream_rate):
        StreamHandler.stream_rate = stream_rate

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
        print file
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
                print 'chunksdir', chunksdir
                print 'framedir', framedir
                print 'path', path
                # get chunks list and open up all files
                files = self.get_chunk_files(path, chunks)
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)

            producer = self.chunkproducer(files, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
            return
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
                    print path
                    filepath = path + '/' # INCOMPLETE. Need to decide on file format.
                    # get chunks list and open up all files
                    files = self.get_chunk_files(path)
                except OSError, err:
                    why = ftpserver._strerror(err)
                    self.respond('550 %s.' % why)

                producer = self.chunkproducer(files, self._current_type)
                self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
                print self._current_type
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
            if True:
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

    def get_chunk_files(self, path, chunks=None):
        """For the specified path, open up all files for reading. and return
        an array of file objects opened for read."""
        iterator = self.run_as_current_user(self.fs.get_list_dir, path)
        files = Queue.Queue()
        if chunks:
            for x in xrange(self.max_chunks):
                try:
                    liststr = iterator.next()
                    filename = ((liststr.split(' ')[-1]).split('\r'))[0]
                    chunk_num = (filename.split('_')[0]).split('.')[-1]
                    if chunk_num.isdigit() and int(chunk_num) in chunks:
                        filepath = path + '/' + filename
                        print filepath
                        fd = self.run_as_current_user(self.fs.open, filepath, 'rb')
                        files.put(fd)
                except StopIteration, err:
                    why = _strerror(err)
                    self.respond('544 %s' %why)
                    break
            return files
        for x in xrange(self.max_chunks):
            try:
                liststr = iterator.next()
                filename = ((liststr.split(' ')[-1]).split('\r'))[0]
                filepath = path + '/' + filename
                print filepath
                fd = self.run_as_current_user(self.fs.open, filepath, 'rb')
                files.put(fd)
            except StopIteration, err:
                why = _strerror(err)
                self.respond('544 %s' %why)
                break
        return files

    def ftp_VLEN(self, filename):
        """Checks the total frames available on this server for the desired
        movie."""
        print filename
        # strip directories and "file-" extension
        fileformat = ((filename.split('/'))[-1]).split('-')
        if fileformat[0] != 'file' or len(fileformat) != 2:
            why = "Format to VLEN should be file-<filename>."
            self.respond('544 %s' %why)
            return
          # /home/ec2-user//movies chunks-OnePiece575/OnePiece575.1.dir
        path2 = path + '/chunks-' + fileformat[1]
        print path2
        iterator = self.run_as_current_user(self.fs.get_list_dir, path2)
        count = 0
        loops = 5000
        for x in xrange(loops):
            try:
                next = iterator.next()
                file_format = next.split('.dir')
                if len(file_format) > 1:
                    count += 1
            except StopIteration:
                break
        self.push_dtp_data(str(count), isproducer=False, cmd="VLEN")

    def ftp_CNKS(self, line):
        """
        FTP command: Returns this cache's chunk number set.
        """
        # hard-coded in right now.
        chunks = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39]
        data = str(chunks)
        self.push_dtp_data(data, isproducer=False, cmd="CNKS")

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
        if True:
            print "Calling _on_dtp_connection."
        return super(StreamHandler, self)._on_dtp_connection()

class FileStreamProducer(ftpserver.FileProducer):
    """
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
        """
        No longer need to restrict the buffer, as ThrottledDTPHandler
        takes care of streaming rate.

        This function sets the size of data to be sent across the TCP conn.
        That is, it is the size of the TCP packet (minus header).
        """
        FileStreamProducer.buffer_size = buffer_size

    @staticmethod
    def set_wait_time(wait_time):
        FileStreamProducer.wait_time = wait_time

    def more(self):
        time.sleep(self.wait_time)
        data = super(FileStreamProducer, self).more()
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
        """
        No longer need to restrict the buffer, as ThrottledDTPHandler
        takes care of streaming rate.

        This function sets the size of data to be sent across the TCP conn.
        That is, it is the size of the TCP packet (minus header).
        """
        FileChunkProducer.buffer_size = buffer_size

    def more(self):
        if self.curr_producer:
            data = self.curr_producer.more()
            if not data:
                if not self.file_queue.empty():
                    f = self.file_queue.get()
                    self.curr_producer = FileStreamProducer( \
                        f, self.type, self.buffer_size)
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
    authorizer.add_user(user, pw, path, perm='elr')
    # allow anonymous login.
    authorizer.add_anonymous(path, perm='elr')

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
    """
    stream_rate = 10000 # 10KB per sec
    StreamHandler.set_stream_rate(stream_rate)
    print "StreamHandler now has size ", StreamHandler.stream_rate

    authorizer = ftpserver.DummyAuthorizer()
    # allow anonymous login.
    print path
    authorizer.add_anonymous(path, perm='elr')
    handler = StreamHandler
    handler.authorizer = authorizer
    # handler.masquerade_address = '107.21.135.254' # Nick EC2
    # handler.masquerade_address = '174.129.174.31' # Lisa EC2
    handler.passive_ports = range(60000, 65535)
    ftpd = ftpserver.FTPServer(server_address, handler)
    ftpd.serve_forever()

if __name__ == "__main__":
    print path
    main()
