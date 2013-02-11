from streamer import *
from time import sleep
import re
from threadclient import ThreadClient
from ftplib import error_perm

class P2PUser():

    def __init__(self, packet_size=2504):
        """ Create a new P2PUser.  Set the packet size, instantiate the manager,
        and establish clients.  Currently, the clients are static but will
        become dynamic when the tracker is implemented.
        """
        self.packet_size = packet_size
        # cache_ip = ['107.21.135.254', '107.21.135.254']
        cache_ip = ['174.129.174.31', '10.10.66.9'] # 1: Lisa EC2, local
        # cache_ip = ['174.129.174.31', '10.0.1.4'] # 1: Lisa EC2, Lisa home 
        self.clients = []
        for i in xrange(2):
            self.clients.append(ThreadClient(cache_ip[i], self.packet_size, i))
            # later: ask tracker.
        self.manager = None # TODO: create the manager class to decode/play
        server_ip = '107.21.135.254' # the IP of the central server.
        self.server_client = ThreadClient(server_ip, self.packet_size)

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
            print inst
            self.clients[0].put_instruction(inst + '.' + client0_request_string)
            self.clients[1].put_instruction(inst + '.' + client1_request_string)
            #print len(available_chunks)
            sleep(8)

            server_request = chunks_to_request(client0_request + client1_request, range(0, 40), 10)
            server_request_string = '%'.join(server_request)
            self.server_client.put_instruction(inst + '.' + server_request_string)
            if(True):
                print 'Requesting %s from server' % \
                    (server_request_string)
                # simple load 1 to 10.
                # for i in xrange(len(server_request)):
                #     server_request[i] = str(server_request[i])

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
        if not isinstance(element, str):
            element = str(element)
        if len(ret_list) >= num_ret:
            break
        if not element in A:
            ret_list.append(element)
    return ret_list

if __name__ == "__main__":
    print "Arguments:", sys.argv
    packet_size = 5 * 1024 * 1024
    if len(sys.argv) > 1:
        packet_size = int(sys.argv[1])

    file_name = 'hyunah'
    if len(sys.argv) > 2:
        file_name = sys.argv[2]
    test_user = P2PUser(packet_size)
    test_user.download(file_name, 1)
    for client in self.clients:
        client.put_instruction('QUIT')
