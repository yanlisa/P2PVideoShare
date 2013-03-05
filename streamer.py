import ftplib
from ftplib import FTP
import sys
import os
import datetime
import threading
import Queue
import logging

def parse_chunks(filestr):
    """Returns file name, chunks, and frame number.
    File string format:
        file-<filename>.<framenum>.<chunk1>%<chunk2>%<chunk3>
     """
    if filestr.find('file-') != -1:
        filestr = (filestr.split('file-'))[-1]
        parts = filestr.split('.')
        if len(parts) < 2:
            return None
        filename, framenum = parts[0], parts[1]
        if len(parts) == 3:
            chunks = map(int, (parts[2]).split('%'))
        else:
            chunks = None

        if True:
            print filestr
        return (filename, framenum, chunks)

logging.basicConfig(level=logging.DEBUG)

# Import SOCKS module if it exists, else standard socket module socket
try:
    import SOCKS; socket = SOCKS; del SOCKS # import SOCKS as socket
    from socket import getfqdn; socket.getfqdn = getfqdn; del getfqdn
    from socket import timeout; socket.timeout = timeout; del timeout
except ImportError:
    import socket
from socket import _GLOBAL_DEFAULT_TIMEOUT

class StreamFTP(threading.Thread, FTP, object):
    def __init__(self, host='', user='', passwd='', acct='',
                 timeout=10.0, chunk_size=2504):
        print "DEBUG, host : ", host
        self.instr_queue = Queue.Queue()
        self.resp_queue = Queue.Queue() # responses, in order.
        self.conn = None # connection socket
        self.callback = None
        self.chunks = []
        self.chunk_size = chunk_size
        FTP.__init__(self, timeout = 300)
        host_ip_address = host[0]
        host_port_num = host[1]
        FTP.connect(self, host_ip_address, host_port_num, 3)
        #FTP.__init__(self, host, user, passwd, acct, timeout)
        threading.Thread.__init__(self)

    def set_chunk_size(self, chunk_size):
        self.chunk_size = chunk_size

    def set_chunks(self, chunks):
        self.chunks = chunks

    def get_instr_queue(self):
        return self.instr_queue

    def get_resp_queue(self):
        return self.resp_queue

    def set_callback(self, callback):
        self.callback = callback

    def retrbinary(self, cmd, callback, blocksize=8192, rest=None):
        """
        Called for file transfer.
        """
        self.voidcmd('TYPE I')
        self.conn = self.transfercmd(cmd, rest)
        self.conn.settimeout(self.timeout)
        if self.chunk_size:
            blocksize = self.chunk_size
        try:
            while 1:
                #data = self.conn.recv(blocksize)
                data = self.conn.recv(self.chunk_size)
                if not data:
                    break
                callback(data)
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise
        self.conn.close()
        self.conn = None
        return self.retrresp()

    def retrlines(self, cmd, callback=None):
        """
        Called for all other commands other than file transfer itself.
        """
        response = ''
        # if callback is None: callback = ftplib.print_line
        resp = self.sendcmd('TYPE A')
        self.conn = self.transfercmd(cmd)
        self.conn.settimeout(self.timeout)
        fp = self.conn.makefile('rb')
        while 1:
            line = fp.readline()
            if self.debugging > 2: print '*retr*', repr(line)
            if not line:
                break
            if line[-2:] == ftplib.CRLF:
                line = line[:-2]
            elif line[-1:] == '\n':
                line = line[:-1]
        #    callback(line)
            response += line + "\n"
        fp.close()
        self.conn.close()
        self.conn = None
        self.resp_queue.put(response)
        return self.retrresp()

    def retrresp(self):
        """Can have different responses, so just keep trying."""
        return self.getresp()

    def run(self):
        """
        Continually get instructions from a queue called by intermediary
        thread-client class.
        The intermediary thread-client class can close the recv socket
        arbitrarily, so this while loop needs to catch those exceptions.
        The queue contains strings of FTP instructions.
        """
        self.login('','')
        self.set_pasv(True) # Trying Passive mode
        while 1:
            cmd = self.instr_queue.get()
            fn_name = cmd.split(' ')[0]
            if fn_name == "QUIT":
                self.quit()
                break
            elif fn_name == "RETR":
                fname = cmd.split(' ')[1]
                print fname
                try:
                    resp = self.retrbinary(cmd, self.callback(self.chunk_size, fname))
                    # resp = self.retrbinary(cmd, open(fname, 'wb').write)
                except socket.error:
                    # something strange happened with the connection; most
                    # likely a cache disconnection.  Ask the tracker to
                    # conect me to a new cache.
                    logging.exception("Connection closed.  Related info:" + str(sys.exc_info()[0]))
                    # (connect to other cache)
                    break
                except:
                    # something else happened while running.  Not much is known.
                    # Let the operator know.
                    logging.exception("Unexpected error" + str(sys.exc_info()[0]))
                    break
            elif fn_name == "ABOR":
                resp = self.abort()
            else: # for any other command, call retrlines.
                try:
                    resp = self.retrlines(cmd)
                except socket.error:
                    logging.exception("Connection closed.  Related info: " + str(sys.exc_info()[0]))
                    break
                except:
                    logging.exception("Unexpected error: " + str(sys.exc_info()[0]))
                    break

def runrecv(packet_size, fname):
    ftp = StreamFTP('107.21.135.254', chunk_size=packet_size)
    if True:
        print "StreamFTP now has size ", ftp.chunk_size
    ret_status = ftp.retrlines('LIST')
    # file_to_write = open(fname, 'wb')
    # ret_status = ftp.retrbinary('RETR ' + fname, filecallback(fname, file_to_write))

    ret_status = ftp.retrbinary('RETR ' + fname, chunkcallback( \
            packet_size, fname))
    # file_to_write.close()
    ftp.quit()

if __name__ == "__main__":
    packet_size = 2504
    if len(sys.argv) > 1:
        packet_size = int(sys.argv[1])
    if len(sys.argv) > 2:
        runrecv(packet_size, sys.argv[2])
    else:
        "Please specify filename."
