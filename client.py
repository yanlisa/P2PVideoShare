import ftplib
import sys
import os
import datetime

# Import SOCKS module if it exists, else standard socket module socket
try:
    import SOCKS; socket = SOCKS; del SOCKS # import SOCKS as socket
    from socket import getfqdn; socket.getfqdn = getfqdn; del getfqdn
    from socket import timeout; socket.timeout = timeout; del timeout
except ImportError:
    import socket
from socket import _GLOBAL_DEFAULT_TIMEOUT
 
class StreamFTP(ftplib.FTP, object):
    def __init__(self, host='', user='', passwd='', acct='',
                 timeout=10.0):
        (super(StreamFTP, self)).__init__(host, user, passwd, acct, timeout)

    def retrbinary(self, cmd, callback, blocksize=8192, rest=None):
        """
	Called for file transfer.
	"""
        self.voidcmd('TYPE I')
        conn = self.transfercmd(cmd, rest)
	conn.settimeout(self.timeout)
        while 1:
            data = conn.recv(blocksize)
            if not data:
                break
            callback(data)
        conn.close()
        return self.retrresp()

    def retrlines(self, cmd, callback=None):
        """
	Called for all other commands other than file transfer itself.
	"""
        if callback is None: callback = ftplib.print_line
        resp = self.sendcmd('TYPE A')
        conn = self.transfercmd(cmd)
	conn.settimeout(self.timeout)
        fp = conn.makefile('rb')
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
            print line
            callback(line)
        fp.close()
        conn.close()
        return self.retrresp()

    def retrresp(self):
        "Can have different responses, so just keep trying."
        return self.getresp()

def filecallback(fname, file_to_write):
    """
        Assumes file is already open to write to.
    """
    total_bytes = [0]
    def helper(data):
        total_bytes[0] += sys.getsizeof(data)
        outputStr = "%s: Received %d bytes.\n" % (fname, total_bytes[0])
	sys.stdout.write(outputStr)
	sys.stdout.flush()
	file_to_write.write(data)

    return helper

def runrecv(fname = "billofrights.txt", user='', pw=''):
    ftp = StreamFTP('107.21.135.254')
    ftp.login(user, pw)
    ftp.set_pasv(True) # Trying Passive mode
    # ret_status = ftp.retrlines('LIST')
    file_to_write = open(fname, 'wb')
    # ret_status = ftp.retrbinary('RETR ' + fname, ftplib.print_line)
    ret_status = ftp.retrbinary('RETR ' + fname, filecallback(fname, file_to_write))
    file_to_write.close()
    ftp.quit()

if __name__ == "__main__":
    runrecv()
