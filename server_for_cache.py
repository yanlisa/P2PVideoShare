import urllib2
import asyncore
import csv
import traceback
import sys, errno
from pyftpdlib import ftpserver
import Queue, time, re
import threading
import threadclient
from helper import parse_chunks, MovieLUT, load_tracker_address

# Debugging MSG
DEBUGGING_MSG = True
# Cache Configuration
server_address = ("localhost", 61001)
tracker_address = load_tracker_address() 
path = "."
movie_config_file = '../config/video_info.csv'

class StreamFTPServer(ftpserver.FTPServer):
    """One instance of the server is created every time this file is run.
    On a new client connection, the server makes a new FTP connection handler.
    Here, that handler is called StreamHandler.

    For each FTP connection handler, when a new transfer request is made,
    PASV mode is set (passive conn handler created), and then on transfer
    instantiation a DTP handler is created.

    handle_accept: on new client connection.
    """
    stream_rate = 10000 # default rate

    def __init__(self, address, handler, spec_rate=0):
        super(StreamFTPServer, self).__init__(address, handler)
        if spec_rate != 0:
            self.stream_rate = spec_rate
            if DEBUGGING_MSG:
                print "[server.py] StreamFTPServer stream rate : ", self.stream_rate
        self.conns = []
        self.handlers = []


    def handle_accept(self):
        """Mainly copy-pasted from FTPServer code. Added stream_rate parameter
        to passive_dtp instantiator.
        """
        """Called when remote client initiates a connection."""
        try:
            sock, addr = self.accept()
        except TypeError:
            # sometimes accept() might return None (see issue 91)
            return
        except socket.error, err:
            # ECONNABORTED might be thrown on *BSD (see issue 105)
            if err.args[0] != errno.ECONNABORTED:
                ftpserver.logerror(traceback.format_exc())
            return
        else:
            # sometimes addr == None instead of (ip, port) (see issue 104)
            if addr is None:
                return

        handler = None
        ip = None
        try:
            """
            *********************
            handler = StreamHandler, which specifies stream_rate for the overall
            tcp connection.

            stream_rate is adjusted with handler.set_stream_rate.
            *********************
            """
            handler = self.handler(sock, self, len(self.handlers), self.stream_rate)
            if not handler.connected:
                return
            ftpserver.log("[]%s:%s Connected." % addr[:2])
            ip = addr[0]
            self.ip_map.append(ip)

            # For performance and security reasons we should always set a
            # limit for the number of file descriptors that socket_map
            # should contain.  When we're running out of such limit we'll
            # use the last available channel for sending a 421 response
            # to the client before disconnecting it.
            if self.max_cons and (len(asyncore.socket_map) > self.max_cons):
                print "Connection accepted for max_cons"
                handler.handle_max_cons()
                return

            # accept only a limited number of connections from the same
            # source address.
            if self.max_cons_per_ip:
                if self.ip_map.count(ip) > self.max_cons_per_ip:
                    handler.handle_max_cons_per_ip()
                    print "Connection accepted for max_cons_per_ip"
                    return

            try:
                handler.handle()
            except:
                handler.handle_error()
        except (KeyboardInterrupt, SystemExit, asyncore.ExitNow):
            raise
        except:
            # This is supposed to be an application bug that should
            # be fixed. We do not want to tear down the server though
            # (DoS). We just log the exception, hoping that someone
            # will eventually file a bug. References:
            # - http://code.google.com/p/pyftpdlib/issues/detail?id=143
            # - http://code.google.com/p/pyftpdlib/issues/detail?id=166
            # - https://groups.google.com/forum/#!topic/pyftpdlib/h7pPybzAx14
            ftpserver.logerror(traceback.format_exc())
            if handler is not None:
                handler.close()
            else:
                if ip is not None and ip in self.ip_map:
                    self.ip_map.remove(ip)
        print "Connection accepted."
        self.conns.append((handler.remote_ip, handler.remote_port))
        self.handlers.append(handler)

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

    # Change PassiveDTP connection handler to handle variable streaming rate.
    # On every PASV request (all requested DLs for anon users), create a new
    # VariablePassiveDTP connection handler. On every transfer start, PassiveDTP
    # connection handler creates a ThrottledDTPHandler. Here, again, to
    # accommodate variable streaming rate, use VariableThrottledDTPHandler.

    def __init__(self, conn, server, index=0, spec_rate=0):
        (super(StreamHandler, self)).__init__(conn, server)
        self._close_connection = False
        self.producer = ftpserver.FileProducer
        self.passive_dtp = VariablePassiveDTP
        self.dtp_handler = VariableThrottledDTPHandler
        self.dtp_handler.read_limit = self.stream_rate  # b/sec (ex 30Kbps = 30*1024)
        self.dtp_handler.write_limit = self.stream_rate # b/sec (ex 30Kbps = 30*1024)
        self.chunkproducer = FileChunkProducer
        self.proto_cmds = proto_cmds
        self.index = index # user connection number

        if spec_rate != 0:
            self.stream_rate = spec_rate
            if DEBUGGING_MSG:
                print "Streaming FTP Handler stream rate:", self.stream_rate

        self.chunks = range(0, self.max_chunks)

    def get_chunks(self):
        return self.chunks

    def set_stream_rate(self, spec_rate):
        if spec_rate != 0:
            self.stream_rate = spec_rate
            if DEBUGGING_MSG:
                print "Streaming FTP Handler stream rate changed to:", self.stream_rate

    def on_connect(self):
        print '[server.py] ******** CONNECTION ESTABLISHED'

    @staticmethod
    def set_movies_path(path):
        StreamHandler.movies_path = path

    def _make_epasv(self, extmode=False):
        """Mainly copy-pasted from FTPServer code. Added stream_rate parameter
        to passive_dtp instantiator.
        """
        """Initialize a passive data channel with remote client which
        issued a PASV or EPSV command.
        If extmode argument is True we assume that client issued EPSV in
        which case extended passive mode will be used (see RFC-2428).
        """
        # close establishing DTP instances, if any
        self._shutdown_connecting_dtp()

        # close established data connections, if any
        if self.data_channel is not None:
            self.data_channel.close()
            self.data_channel = None

        # make sure we are not hitting the max connections limit
        if self.server.max_cons:
            if len(asyncore.socket_map) >= self.server.max_cons:
                msg = "Too many connections. Can't open data channel."
                self.respond("425 %s" %msg)
                self.log(msg)
                return

        # open data channel
        self._dtp_acceptor = self.passive_dtp(self, extmode, self.stream_rate)

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).

        Accepts filestrings of the form:
            chunk-<filename>.<ext>&<framenum>/<chunknum>
            file-<filename>
        """
        parsedform = parse_chunks(file)
        if parsedform:
            filename, framenum, binary_g, chunks = parsedform
            try:
                # filename should be prefixed by "file-" in order to be valid.
                # frame number is expected to exist for this cache.
                chunksdir = 'video-' + filename
                framedir = filename + '.' + framenum + '.dir'
                path = self.movies_path + '/' + chunksdir + '/' + framedir
                # get chunks list and open up all files
                files = self.get_chunk_files(path, chunks)

                # if DEBUGGING_MSG:
                #     print "chunks requested:", chunks
                #     print 'chunksdir', chunksdir
                #     print 'framedir', framedir
                #     print 'path', path
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)

            producer = self.chunkproducer(files, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
            return

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
        video_name = filename.split('file-')[-1]
        vlen_items = [self.movie_LUT.frame_num_lookup(video_name),
                    self.movie_LUT.size_bytes_lookup(video_name),
                    self.movie_LUT.chunk_size_lookup(video_name),
                    self.movie_LUT.last_chunk_size_lookup(video_name)]
        vlen_str = '&'.join(map(str, vlen_items))
        self.push_dtp_data(vlen_str, isproducer=False, cmd="VLEN")
        print vlen_str

    def ftp_CNKS(self, line):
        """
        FTP command: Returns this cache's chunk number set.
        """
        # hard-coded in right now.
        data = str(self.chunks)
        data = data + '&' + str(self.max_chunks)
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
        return super(StreamHandler, self)._on_dtp_connection()


class VariablePassiveDTP(ftpserver.PassiveDTP):
    """
    Inherits from PassiveDTP; can specify streaming rate.
    """

    stream_rate = 10*1024
    def __init__(self, cmd_channel, extmode=False, spec_rate=0):
        super(VariablePassiveDTP, self).__init__(cmd_channel, extmode)
        if spec_rate != 0:
            self.stream_rate = spec_rate
            if DEBUGGING_MSG:
                print "VariablePassiveDTP stream rate:", self.stream_rate

    def handle_accept(self):
        """
        ON PASSIVE DTP CREATION, NOT ON INITIAL TCP CONNECTION.
        For Initial TCP connection, see handle_accept in StreamFTPServer.
        Mainly copy-pasted from PassiveDTP, except that dtp_handler is run
        with a stream_rate.
        """
        """Called when remote client initiates a connection."""
        if not self.cmd_channel.connected:
            return self.close()
        try:
            sock, addr = self.accept()
        except TypeError:
            # sometimes accept() might return None (see issue 91)
            return
        except socket.error, err:
            # ECONNABORTED might be thrown on *BSD (see issue 105)
            if err.args[0] != errno.ECONNABORTED:
                self.log_exception(self)
            return
        else:
            # sometimes addr == None instead of (ip, port) (see issue 104)
            if addr == None:
                return

        # Check the origin of data connection.  If not expressively
        # configured we drop the incoming data connection if remote
        # IP address does not match the client's IP address.
        if self.cmd_channel.remote_ip != addr[0]:
            if not self.cmd_channel.permit_foreign_addresses:
                try:
                    sock.close()
                except socket.error:
                    pass
                msg = 'Rejected data connection from foreign address %s:%s.' \
                        %(addr[0], addr[1])
                self.cmd_channel.respond("425 %s" % msg)
                self.log(msg)
                # do not close listening socket: it couldn't be client's blame
                return
            else:
                # site-to-site FTP allowed
                msg = 'Established data connection with foreign address %s:%s.'\
                        % (addr[0], addr[1])
                self.log(msg)
        # Immediately close the current channel (we accept only one
        # connection at time) and avoid running out of max connections
        # limit.
        self.close()
        # delegate such connection to DTP handler
        if self.cmd_channel.connected:
            handler = self.cmd_channel.dtp_handler(sock, self.cmd_channel, self.stream_rate)
            if handler.connected:
                self.cmd_channel.data_channel = handler
                self.cmd_channel._on_dtp_connection()

class VariableThrottledDTPHandler(ftpserver.ThrottledDTPHandler):
    """
    Inherits from ThrottledDTPHandler; can specify streaming rate.
    """
    def __init__(self, sock_obj, cmd_channel, spec_rate=0):
        super(VariableThrottledDTPHandler, self).__init__(sock_obj, cmd_channel)
        if spec_rate != 0:
            self.read_limit = spec_rate
            self.write_limit = spec_rate
            if DEBUGGING_MSG:
                print "VariableThrottledDTP stream rate:", self.write_limit

    def auto_size_buffers(self, spec_rate):
        if self.read_limit:
            while self.ac_in_buffer_size > self.read_limit:
                self.ac_in_buffer_size /= 2
        if self.write_limit:
            while self.ac_out_buffer_size > self.write_limit:
                self.ac_out_buffer_size /= 2

    def recv(self, buffer_size, spec_rate=0):
        if spec_rate != 0:
            self.read_limit = spec_rate
            self.write_limit = spec_rate
            if self.auto_sized_buffers:
                self.auto_size_buffers(spec_rate)
        return super(VariableThrottledDTPHandler, self).recv(buffer_size)

    def send(self, data, spec_rate=0):
        if spec_rate != 0:
            self.read_limit = spec_rate
            self.write_limit = spec_rate
            if self.auto_sized_buffers:
                self.auto_size_buffers(spec_rate)
        return super(VariableThrottledDTPHandler, self).send(data)


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

def main():
    """Parameters:
        No parameters: run with defaults (assume on ec2server)
    """
    stream_rate = 100000000 # 30KB per sec
    authorizer = ftpserver.DummyAuthorizer()
    # allow anonymous login.
    if DEBUGGING_MSG:
        print '[server.py] Path : ', path
    authorizer.add_anonymous(path, perm='elr')
    handler = StreamHandler
    handler.authorizer = authorizer

    # Register server to tracker
    req_str = 'REGISTER_SERVER_FOR_CACHE&' + server_address[0] + '_' + str(server_address[1])
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    print ret_str
    if not ret_str == 'Server is registered':
        err_msg = 'Server failed to be registered'
        print err_msg
        return err_msg

 # handler.masquerade_address = '107.21.135.254' # Nick EC2
    # handler.masquerade_address = '174.129.174.31' # Lisa EC2
    handler.passive_ports = range(60000, 65535)
    ftpd = StreamFTPServer(server_address, handler, stream_rate)
    ftpd.serve_forever()

if __name__ == "__main__":
    main()
