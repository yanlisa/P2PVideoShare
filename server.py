import sys, errno
from pyftpdlib import ftpserver
import Queue, time

class StreamHandler(ftpserver.FTPHandler):
    def __init__(self, conn, server, wait_time=1):
        (super(StreamHandler, self)).__init__(conn, server)
        self._close_connection = False
        self._wait_time = wait_time
        self._packet_size = 100

    def ftp_RETR(self, file):
        """
        Copied and pasted the code from ftp_RETR here, because we need to set
        the offset of the file reading, and not just handle the ftp_REST case.
        """
        offset_pos = self._restart_position
        self._restart_position = 0
        while offset_pos < self.fs.getsize(file):
            try:
                try:
                    fd = self.run_as_current_user(self.fs.open, file, 'rb')
                except (EnvironmentError, ftpserver.FilesystemError):
                    err = sys.exc_info()[1]
                    why = _strerror(err)
                    self.respond('550 %s.' % why)
                    return

                if offset_pos:
                    # Make sure that the requested offset is valid (within the
                    # size of the file being resumed).
                    # According to RFC-1123 a 554 reply may result in case that
                    # the existing file cannot be repositioned as specified in
                    # the REST.
                    ok = 0
                    try:
                        if offset_pos > self.fs.getsize(file):
                            raise ValueError
                        fd.seek(offset_pos)
                        ok = 1
                    except ValueError:
                        why = "Invalid REST parameter"
                    except (EnvironmentError, FilesystemError):
                        err = sys.exc_info()[1]
                        why = _strerror(err)
                    if not ok:
                        fd.close()
                        self.respond('554 %s' % why)
                        return
                producer = FilePacketProducer(fd, self._current_type, self._packet_size)
                data = fd.read(self._packet_size)
                self.push_dtp_data(producer, isproducer=True, file=fd, cmd="RETR")

            except IOError, err:
                # initial file did not exist. Otherwise assume things
                # are broken into chunks...
                if streaming_count == 1:
                    why = ftpserver._strerror(err)
                    self.respond('550 %s.' % why)
                return

            offset_pos += self._packet_size
        self._close_connection = True

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
            why = _strerror(err)
            self.respond('550 %s.' % why)
        else:
            producer = MovieLister(iterator)
            self.push_dtp_data(producer, isproducer=True, cmd="LIST")

class FilePacketProducer(ftpserver.FileProducer):
    """Wraps around FileProducer such that reading is limited by
    packet_size.
    Default buffer_size is 65536 as specified in FileProducer.
    """
    def __init__(self, file, type, packet_size=65536, wait_time=1):
        self.buffer_size = packet_size
        self.wait_time = wait_time
        super(FilePacketProducer, self).__init__(file, type)

    def more(self):
        return super(FilePacketProducer, self).more()
        time.sleep(self.wait_time)

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
    user = "user" + "1"
    pw = "1"

    authorizer = ftpserver.DummyAuthorizer()
    authorizer.add_user(user, pw, "/home/ec2-user", perm='elr')
    # allow anonymous login.
    authorizer.add_anonymous("/home/ec2-user", perm='elr')

    handler = StreamHandler
    handler.authorizer = authorizer
    handler.masquerade_address = '107.21.135.254'
    handler.passive_ports = range(60000, 65535)
    address = ("10.29.147.60", 21)
    ftpd = ftpserver.FTPServer(address, handler)
    ftpd.serve_forever()

if __name__ == "__main__":
    main(sys.argv)
