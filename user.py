from streamer import *
from time import sleep
import re
from threadclient import ThreadClient
from ftplib import error_perm
from zfec import filefec

# Debugging MSG
DEBUGGING_MSG = True

# Global parameters
CACHE_DOWNLOAD_DURATION = 12 # sec
SERVER_DOWNLOAD_DURATION = 2 # sec

# IP Table
ip_local = 'localhost'
ip_ec2_lisa = '174.129.174.31'
ip_ec2_nick = '107.21.135.254'

# IP Configuration
cache_ip_address = [(ip_ec2_lisa, 21), (ip_local, 22)]
server_ip_address = (ip_ec2_nick, 21)

class P2PUser():

    def __init__(self, packet_size=2504):
        """ Create a new P2PUser.  Set the packet size, instantiate the manager,
        and establish clients.  Currently, the clients are static but will
        become dynamic when the tracker is implemented.
        """
        self.packet_size = packet_size
        cache_ip = cache_ip_address
        self.clients = []
        for i in xrange(len(cache_ip)):
            self.clients.append(ThreadClient(cache_ip[i], self.packet_size, i))
            # later: ask tracker.
        self.manager = None # TODO: create the manager class to decode/play
        server_ip = server_ip_address
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
        base_file_name = video_name + '.flv'
        base_file = open(base_file_name, 'ab')
        for frame_number in xrange(start_frame, video_length):
            filename = 'file-' + video_name + '.' + str(frame_number)

            # get available chunks lists from cache A and B.
            inst = 'CNKS ' + filename
            self.clients[0].put_instruction(inst)
            chunks = self.clients[0].get_response()
            chunks = chunks[1:-2].split(', ')
            # Number of chunks requested.
            client0_request = chunks_to_request([], chunks, 6)
            client0_request_string = '%'.join(client0_request)
            print client0_request_string

            self.clients[1].put_instruction(inst)
            chunks = self.clients[1].get_response()
            chunks = chunks[1:-2].split(', ')
            # Number of chunks requested.
            client1_request = chunks_to_request(client0_request, chunks, 16)
            client1_request_string = '%'.join(client1_request)
            print client1_request_string

            #print 'client available chunks: %s' % (str(chunks))
            #available_chunks = available_chunks | set(chunks)
            #client.set_chunks(str(available_chunks & set(chunks)))
            inst = 'RETR ' + filename
            print inst
            self.clients[0].put_instruction(inst + '.' + client0_request_string)
            self.clients[1].put_instruction(inst + '.' + client1_request_string)
            #print len(available_chunks)
            sleep(CACHE_DOWNLOAD_DURATION)

            # immediately stop cache downloads.
            (self.clients[0]).client.abort()
            (self.clients[1]).client.abort()

            # Look up the download directory and count the downloaded chunks


            # Request from server remaining chunks missing

            # TODO: Find the chunks received so far using OS.
            # First argument: chunks received, Second: server chunks (everything), Third: # chunks needed
            # third arg: k (= 20) - len(first arg)
            total_num_of_chunks_rx = len(client0_request + client1_request)
            if (total_num_of_chunks_rx >= 1000):
                print 'Nothing to download from the server :D'
            else:
                server_request = chunks_to_request(client0_request + client1_request, range(0, 40), 5)
                server_request_string = '%'.join(server_request)
                self.server_client.put_instruction(inst + '.' + server_request_string)
                if(DEBUGGING_MSG):
                    print 'Requesting %s from server' % \
                        (server_request_string)
                    # simple load 1 to 10.
                    # for i in xrange(len(server_request)):
                    #     server_request[i] = str(server_request[i])

                # put together chunks into single frame; then concatenate onto original file.
            sleep(SERVER_DOWNLOAD_DURATION)

            # abort the connection to the server
            self.server_client.client.abort()

            print 'about to decode...'
            folder_name = video_name + '.' + str(frame_number) + '/'
            chunksList = []
            for chunk in os.listdir(folder_name):
                chunkFile = open(folder_name + chunk, 'rb')
                chunksList.append(chunkFile)
            if frame_number != start_frame:
                print 'size of base file:', os.path.getsize(base_file_name)
            print 'trying to decode'
            filefec.decode_from_files(base_file, chunksList)
            print 'decoded.  Size of base file =', os.path.getsize(base_file_name)

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
