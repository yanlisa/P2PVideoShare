import sys, errno
from pyftpdlib import ftpserver
import Queue, time

class StreamHandler(ftpserver.FTPHandler):
    def __init__(self, conn, server, wait_time=1):
        (super(StreamHandler, self)).__init__(conn, server)
        self._out_dtp_queue = Queue.Queue()
        self._close_connection = False
        self._wait_time = wait_time
        self._packet_size = 100

    def close(self):
        super(StreamHandler, self).close()
        self._out_dtp_queue = Queue.Queue()

    def _on_dtp_connection(self):
        """Called every time data channel connects, either active or
        passive.

        Incoming and outgoing queues are checked for pending data.
        If outbound data is pending, it is pushed into the data channel.
        If awaiting inbound data, the data channel is enabled for
        receiving.
        """
        # Close accepting DTP only. By closing ActiveDTP DTPHandler
        # would receive a closed socket object.
        #self._shutdown_connecting_dtp()
        if self._dtp_acceptor is not None:
            self._dtp_acceptor.close()
            self._dtp_acceptor = None

        # stop the idle timer as long as the data transfer is not finished
        if self._idler is not None and not self._idler.cancelled:
            self._idler.cancel()

        # check for data to send
        if not self._out_dtp_queue.empty():
            #while not self._out_dtp_queue.empty():
                data, isproducer, file, cmd = self._out_dtp_queue.get()
                self.data_channel.cmd = cmd
                if file:
                    self.data_channel.file_obj = file
                try:
                    if not isproducer:
                        self.data_channel.push(data)
                    else:
                        self.data_channel.push_with_producer(data)
                    time.sleep(self._wait_time)
                except:
                    # dealing with this exception is up to DTP (see bug #84)
                    self.data_channel.handle_error()

        # check for data to receive
        if self._in_dtp_queue is not None:
            file, cmd = self._in_dtp_queue
            self.data_channel.file_obj = file
            self._in_dtp_queue = None
            self.data_channel.enable_receiving(self._current_type, cmd)

        #if self._close_connection:
        #    self.close_when_done()
        #    self._close_connection = False

    def push_dtp_data(self, data, isproducer=False, file=None, cmd=None):
        """Pushes data into the data channel.

        It is usually called for those commands requiring some data to
        be sent over the data channel (e.g. RETR).
        If data channel does not exist yet, it queues the data to send
        later; data will then be pushed into data channel when
        _on_dtp_connection() will be called.

         - (str/classobj) data: the data to send which may be a string
            or a producer object).
         - (bool) isproducer: whether treat data as a producer.
         - (file) file: the file[-like] object to send (if any).
        """
        if self.data_channel is not None:
            self.respond("125 Data connection already open. Transfer starting.")
            if file:
                self.data_channel.file_obj = file
            try:
                if not isproducer:
                    self.data_channel.push(data)
                else:
                    self.data_channel.push_with_producer(data)
                if self.data_channel is not None:
                    self.data_channel.cmd = cmd
            except:
                # dealing with this exception is up to DTP (see bug #84)
                self.data_channel.handle_error()
        else:
            self.respond("150 File status okay. About to open data connection.")
            self._out_dtp_queue.put((data, isproducer, file, cmd))

    def flush_account(self):
        super(StreamHandler, self).flush_account()
        self._out_dtp_queue = Queue.Queue()

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
                self.push_dtp_data(data, isproducer=False, file=fd, cmd="RETR")

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
     def __init__(self, file, type, packet_size=65536):
        self.buffer_size = packet_size
        super(FilePacketProducer, self).__init__(file, type)

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
