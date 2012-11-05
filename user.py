from streamer import *
from time import sleep
import re
from threadclient import ThreadClient
from ftplib import error_perm

class P2PUser():

    def __init__(self):
        server_ip = '192.168.0.120'
        self.clients = []
        for i in xrange(2):
            self.clients.append(ThreadClient(server_ip, 2504)) # later: ask tracker
        self.manager = None # TODO: create the manager class to decode/play
        server_ip = '107.21.135.254'
        self.server_client = ThreadClient(server_ip, 2504)

    def play(self, video_name, frame_number):
        """ Starts playing the video as identified by either name or number and
        begins handling the data connections necessary to play the video,
        starting at frame_number (the 10-second section of time within the
        vid).
        """
        # inform the web browser we have started playing
        if not self.manager.playing():
            self.manager.start_playing()
        # TODO: add decoding.

    def download(self, video_name, frame_number):
        # ask all clients for all chunks in the next 10 second set
        # after 10 sec, cancel client connections
        available_chunks = set([])
        done = False
        while not done:
            for client in self.clients:
                filename = 'file-' + video_name + '.' + str(frame_number)
                inst = 'CNKS ' + filename
                client.put_instruction(inst)
                chunks = client.get_response()
                chunks = chunks[1:-2].split(', ')
                for i in xrange(len(chunks)):
                    chunks[i] = int(chunks[i])
                print 'client available chunks: %s' % (str(chunks))
                available_chunks = available_chunks | set(chunks)
                client.set_chunks(str(available_chunks & set(chunks)))
                inst = 'RETR ' + filename
                client.put_instruction(inst)
                print len(available_chunks)
                if len(available_chunks) >= 20:
                    done = True
                    break
            sleep(8)
            if(False): # check if I downloaded enough packets from my peers.
                server_client.put_instruction(inst)
            frame_number += 1

if __name__ == "__main__":
    test_user = P2PUser()
    test_user.download('OO1rH.jpg', 1)