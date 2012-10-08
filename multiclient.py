import ftplib
import sys
import os
import datetime
import threading
import client

"""
Test class. Allows multiple users to be created on one computer. All users
will query for a file download on the current computer.
"""

class ClientWrapper(threading.Thread):
    """
    	Multi-client enabler. For each instance of ClientWrapper,
	run a file-requester.
    """
    def __init__(self, fname="billofrights.txt"):
        self.fname = fname
	self.user = ''
	self.pw = ''
	threading.Thread.__init__(self)
	# super(threading.Thread, self).__init__()

    def run(self):
        client.runrecv(self.fname, self.user, self.pw)

if __name__ == "__main__":
    """
    Default command prompt call will run a 3-user scenario.
    """
    num_users = 3
    if len(sys.argv) == 2:
        num_users = int(sys.argv[1])
    users = [0]*num_users
    files = ['billofrights.txt', 'Abracadabra.mp3', 'hyunah.flv']
    for i in range(num_users):
        users[i] = ClientWrapper(files[i])
	(users[i]).start()
