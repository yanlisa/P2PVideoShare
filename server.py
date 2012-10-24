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
        self.producer = FilePacketProducer
        self.producer.set_buffer_size(self.packet_size)

    @staticmethod
    def set_packet_size(packet_size):
        StreamHandler.packet_size = packet_size

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client)
        """
        movies_path = '/home/ec2-user/movies'
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

class FilePacketProducer(ftpserver.FileProducer):
    """Wraps around FileProducer such that reading is limited by
    packet_size.
    Default buffer_size is 65536 as specified in FileProducer.
    """
    def __init__(self, file, type, packet_size=65536, wait_time=1):
        self.wait_time = wait_time
        super(FilePacketProducer, self).__init__(file, type)

    @staticmethod
    def set_buffer_size(buffer_size):
        FilePacketProducer.buffer_size = buffer_size

    def more(self):
        time.sleep(self.wait_time)
        data = super(FilePacketProducer, self).more()
        outputStr = "Size of packet to send: %d\n" % sys.getsizeof(data)
        sys.stdout.write(outputStr)
        sys.stdout.flush()
        return data

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

def main(user_params):
    authorizer = ftpserver.DummyAuthorizer()
    # allow anonymous login.
    authorizer.add_anonymous("/home/ec2-user", perm='elr')

    if len(sys.argv) == 2:
        packet_size = int(sys.argv[1])
        StreamHandler.set_packet_size(packet_size)
    handler = StreamHandler
    handler.authorizer = authorizer
    handler.masquerade_address = '107.21.135.254'
    handler.passive_ports = range(60000, 65535)
    address = ("10.29.147.60", 21)
    ftpd = ftpserver.FTPServer(address, handler)
    ftpd.serve_forever()

if __name__ == "__main__":
    main(sys.argv)
