import ftplib
from ftplib import FTP
import sys
import os
import datetime
import threading
import Queue

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
                 timeout=10.0):
        self.instr_queue = Queue.Queue()
        self.conn = None # connection socket
        self.callback = None
        FTP.__init__(self, host, user, passwd, acct, timeout)
        threading.Thread.__init__(self)

    def set_chunk_size(self, chunk_size):
        self.chunk_size = chunk_size

    def get_queue(self):
        return self.instr_queue

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
                data = self.conn.recv(blocksize)
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
        if callback is None: callback = ftplib.print_line
        resp = self.sendcmd('TYPE A')
        self.conn = self.transfercmd(cmd)
        self.conn.settimeout(self.timeout)
        fp = self.conn.makefile('rb')
        print cmd, 'returned:'
        while 1:
            line = fp.readline()
            if self.debugging > 2: print '*retr*', repr(line)
            if not line:
                break
            if line[-2:] == ftplib.CRLF:
                line = line[:-2]
            elif line[-1:] == '\n':
                line = line[:-1]
            callback(line)
        fp.close()
        self.conn.close()
        self.conn = None
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
                try:
                    resp = self.retrbinary(cmd, self.callback(self.chunk_size, fname))
                except socket.error:
                    print "Connection closed."
                except:
                    print "Unexpected error:", sys.exc_info()[0]
            elif fn_name == "LIST":
                try:
                    resp = self.retrlines(cmd)
                except socket.error:
                    print "Connection closed."
                except:
                    print "Unexpected error:", sys.exc_info()[0]

def runrecv(packet_size, fname):
    ftp = StreamFTP('107.21.135.254')
    ftp.set_packet_size(packet_size)
    print "StreamFTP now has size ", StreamFTP.packet_size
    ret_status = ftp.retrlines('LIST')
    # file_to_write = open(fname, 'wb')
    # ret_status = ftp.retrbinary('RETR ' + fname, filecallback(fname, file_to_write))

    ret_status = ftp.retrbinary('RETR ' + fname, chunkcallback( \
            packet_size, fname))
    # file_to_write.close()
    ftp.quit()

if __name__ == "__main__":
    packet_size = 2500
    if len(sys.argv) > 1:
        packet_size = int(sys.argv[1])
    if len(sys.argv) > 2:
        runrecv(packet_size, sys.argv[2])
    else:
        "Please specify filename."
