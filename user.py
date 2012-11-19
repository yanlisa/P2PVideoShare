from streamer import *
from time import sleep
import re
from threadclient import ThreadClient
from ftplib import error_perm

class P2PUser():

    def __init__(self):
        server_ip = '10.10.65.3'
        self.clients = []
        for i in xrange(2):
            self.clients.append(ThreadClient(server_ip, 2504, i)) # ask tracker
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

    def download(self, video_name, start_frame):
        # ask all clients for 5 chunks in the next 10 second set
        # make sure each of these requests have separate chunk numbers.
        # after receiving these packets, get stuff from the server.
        available_chunks = set([])
        self.clients[0].put_instruction('VLEN file-%s' % (video_name))
        video_length = int(self.clients[0].get_response())
        for frame_number in xrange(start_frame, video_length):
            filename = 'file-' + video_name + '.' + str(frame_number)

            # get available chunks lists from cache A and B.
            inst = 'CNKS ' + filename
            self.clients[0].put_instruction(inst)
            chunks = self.clients[0].get_response()
            chunks = chunks[1:-2].split(', ')
            client0_request = chunks_to_request([], chunks, 5)
            client0_request_string = '%'.join(client0_request)

            self.clients[1].put_instruction(inst)
            chunks = self.clients[1].get_response()
            chunks = chunks[1:-2].split(', ')
            client1_request = chunks_to_request(client0_request, chunks, 5)
            client1_request_string = '%'.join(client1_request)

            #print 'client available chunks: %s' % (str(chunks))
            #available_chunks = available_chunks | set(chunks)
            #client.set_chunks(str(available_chunks & set(chunks)))

            inst = 'RETR ' + filename
            self.clients[0].put_instruction(inst + '.' + client0_request_string)
            self.clients[1].put_instruction(inst + '.' + client1_request_string)
            #print len(available_chunks)
            sleep(8)
 
            if(True):
                server_request = chunks_to_request(client0_request + client1_request, range(1, 41), 10)
                print 'finished downloading from clients.  Requesting %s from server' % (server_request)
                for i in xrange(len(server_request)):
                    server_request[i] = str(server_request[i])
                server_request = '%'.join(server_request)
                self.server_client.put_instruction(inst + '.' + server_request)

def chunks_to_request(A, B, num_ret):
    """ Find the elements in B that are not in A. From these elements, return a
    randomized set that has maximum num_ret elements.

    Example: A = {1, 3, 5, 7, 9}, B = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14},
    num_ret = 5 possible element sets: {2, 4, 6, 8, 10}, {2, 4, 6, 8, 12}, and
    so on.
    
    For now, it may just be easiest to take the first num_ret elements of the
    non-overlapping set instead of randomizing the elements to choose from the
    non-overlapping set. """
    #intersection = set(A) & set(B)
    ret_list = []
    for element in B:
        if len(ret_list) >= num_ret:
            break
        if not element in A:
            ret_list.append(element)
    return ret_list

if __name__ == "__main__":
    test_user = P2PUser()
    test_user.download('Abracadabra', 1)
    for client in self.clients:
        client.put_instruction('QUIT')
