import sys, errno
from pyftpdlib import ftpserver
import Queue, time, re

class StreamHandler(ftpserver.FTPHandler):
    packet_size = 100 # default (in bytes)

    def __init__(self, conn, server, wait_time=1):
        (super(StreamHandler, self)).__init__(conn, server)
        self._close_connection = False
        self._wait_time = wait_time
        self.dtp_handler = DTPPacketHandler
        self.dtp_handler.set_buffer_size(self.packet_size)
        self.producer = FileStreamProducer
        self.producer.set_buffer_size(self.packet_size)

        # change
        self.chunkproducer = FileStreamProducer
    @staticmethod
    def set_packet_size(packet_size):
        StreamHandler.packet_size = packet_size

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).

        If the file has an integer extension, assume it is asking for a
        file frame. cd into the correct directory and transmit all chunks
        the server has for that frame.
        """
        movies_path = '/home/ec2-user/movies'

        ext = (file.split('.'))[-1]
        if ext.isdigit():
            try:
                # filename should be prefixed by "file-" in order to be valid.
                # frame number is expected to exist for this cache.
                filename=(((file.split('.'))[0]).split('file-'))[1]
                chunksdir = 'chunks-' + filename
                framedir = filename + '.' + ext + '.dir'
                path = movies_path + '/' + chunksdir + '/' + framedir
                iterator = self.run_as_current_user(self.fs.get_list_dir, path)
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)
    
            producer = self.chunkproducer(file, self._current_type)
            return

        rest_pos = self._restart_position
        self._restart_position = 0
        try:
            fd = self.run_as_current_user(self.fs.open, file, 'rb')
        except (EnvironmentError, FilesystemError):
            err = sys.exc_info()[1]
            why = _strerror(err)
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

    def ftp_LIST(self, path):
        """Return a list of files in the specified directory to the
        client.
        """
        # - If no argument, fall back on cwd as default.
        # - Some older FTP clients erroneously issue /bin/ls-like LIST
        #   formats in which case we fall back on cwd as default.
        movies_path = '/home/ec2-user/movies'
        try:
            iterator = self.run_as_current_user(self.fs.get_list_dir, movies_path)
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
    """Wraps around FileProducer such that reading is limited by
    packet_size.
    Default buffer_size is 65536 as specified in FileProducer.
    """
    buffer_size = 65535
    wait_time = 1
    def __init__(self, file, type):
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
    def __init__(self, filequeue, type, packet_size=65536, wait_time=1):
        self.file_queue = filequeue
        self.curr_file = None
        self.curr_producer = None

    def more(self):
        if not self.curr_file and not self.file_queue.empty():
            self.curr_file = self.file_queue.get()
            self.curr_producer = super(FileChunkProducer, \
                self).__init__(self.curr_file, type)
        if self.curr_producer:
            # More to send.
            data = self.curr_producer.more()
            if not data:
                self.curr_file = None
                self.curr_producer = None
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
                next = next.split('file-')[-1]
                buffer.append(str(i) + ': ' + next)
                i += 1
            except StopIteration:
                break
        return ''.join(buffer)

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
