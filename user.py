from streamer import *
from time import sleep
import os
import re
import sets
from threadclient import ThreadClient
from ftplib import error_perm
from zfec import filefec

# Debugging MSG
DEBUGGING_MSG = True

# Global parameters
CACHE_DOWNLOAD_DURATION = 8 # sec
SERVER_DOWNLOAD_DURATION = 2 # sec
DECODE_WAIT_DURATION = 0.1 # sec

# IP Table
ip_local = 'localhost'
ip_ec2_lisa = '174.129.174.31'
ip_ec2_nick = '107.21.135.254'

# IP Configuration
#cache_ip_address = [(ip_ec2_lisa, 25), (ip_local, 22)]
cache_ip_address = []
num_of_caches = 1
base_port = 60000
for i in range(num_of_caches):
    cache_ip_address.append((ip_local, base_port+i))
server_ip_address = (ip_local, 61000)

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
            print '[user.py] frame_number : ', frame_number
            filename = 'file-' + video_name + '.' + str(frame_number)
            # directory for this frame
            folder_name = video_name + '.' + str(frame_number) + '/'

            # get available chunks lists from cache A and B.
            inst_CNKS = 'CNKS ' + filename
            inst_RETR = 'RETR ' + filename

            req_so_far = []
            for client in self.clients:
                client.put_instruction(inst_CNKS)
                chunks = client.get_response()
                chunks = chunks[1:-2].split(', ')
                client_request = chunks_to_request(req_so_far, chunks, 3)
                req_so_far = list( set(req_so_far) | set(client_request) )
                client_request_string = '%'.join(client_request)
                client.put_instruction(inst_RETR + '.' + client_request_string)
                print client_request_string
            sleep(CACHE_DOWNLOAD_DURATION)

            #print 'client available chunks: %s' % (str(chunks))
            #available_chunks = available_chunks | set(chunks)
            #client.set_chunks(str(available_chunks & set(chunks)))
            #print len(available_chunks)

            # immediately stop cache downloads.
            for client in self.clients:
                client.client.abort()

            # Look up the download directory and count the downloaded chunks
            chunk_nums_rx = chunk_nums_in_frame_dir(folder_name)
            if (DEBUGGING_MSG):
                print "%d chunks received from caches for frame %d: " % (len(chunk_nums_rx), frame_number)
                print chunk_nums_rx

            # Request from server remaining chunks missing

            # TODO: Find the chunks received so far using OS.
            # First argument: chunks received, Second: server chunks (everything), Third: # chunks needed
            # third arg: k (= 20) - len(first arg)
            num_chunks_rx = len(chunk_nums_rx)
            if (num_chunks_rx >= 20):
                print 'Nothing to download from the server :D'
            else:
                server_request = chunks_to_request(chunk_nums_rx, range(0, 40), 20 - num_chunks_rx)
                if server_request:
                    server_request_string = '%'.join(server_request)
                    self.server_client.put_instruction(inst_RETR + '.' + server_request_string)
                    if(DEBUGGING_MSG):
                        print 'Requesting %s from server' % \
                            (server_request_string)
                elif (DEBUGGING_MSG):
                    print "No unique chunks from server requested."

            sleep(SERVER_DOWNLOAD_DURATION)

            if (DEBUGGING_MSG):
                print "[user.py] Waiting to receive all elements from server."
            while True:
                chunk_nums = chunk_nums_in_frame_dir(folder_name)
                num_chunks_rx = len(chunk_nums)
                if num_chunks_rx >= 20:
                    break
                sleep(DECODE_WAIT_DURATION)

            if (DEBUGGING_MSG):
                print "[user.py] Received 20 packets"

            # abort the connection to the server
            self.server_client.client.abort()

            # put together chunks into single frame; then concatenate onto original file.
            print 'about to decode...'
            chunksList = chunk_files_in_frame_dir(folder_name)

            if frame_number != start_frame:
                print 'size of base file:', os.path.getsize(base_file_name)
            print 'trying to decode'
            filefec.decode_from_files(base_file, chunksList)
            print 'decoded.  Size of base file =', os.path.getsize(base_file_name)
            if frame_number == 1:
                # Open VLC Player
                os.system('/Applications/VLC.app/Contents/MacOS/VLC2 OnePiece575.flv &')

def chunk_nums_in_frame_dir(folder_name):
    # returns an array of chunk numbers (ints) in this frame.
    # folder_name ends in '/'.
    # assumes chunk filenames end in chunk number.
    chunksNums = []
    for chunk_name in os.listdir(folder_name):
        chunk_suffix = (chunk_name.split('.'))[-1]
        if chunk_suffix.isdigit():
            chunksNums.append(chunk_suffix)
    return chunksNums

def chunk_files_in_frame_dir(folder_name):
    # opens file objects for each file in the directory.
    # folder_name ends in '/'.
    chunksList = []
    for chunk_name in os.listdir(folder_name):
        chunkFile = open(folder_name + chunk_name, 'rb')
        chunksList.append(chunkFile)
    return chunksList

def chunks_to_request(A, B, num_ret):
    """ Find the elements in B that are not in A. From these elements, return a
    randomized set that has maximum num_ret elements.

    Example: A = {1, 3, 5, 7, 9}, B = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14},
    num_ret = 5 possible element sets: {2, 4, 6, 8, 10}, {2, 4, 6, 8, 12}, and
    so on.

    For now, it may just be easiest to take the first num_ret elements of the
    non-overlapping set instead of randomizing the elements to choose from the
    non-overlapping set. """
    set_A, set_B = sets.Set(map(str, A)), sets.Set(map(str, B)) # map all elts to str
    list_diff = list(set_B - set_A)
    list_diff.sort()
    return list_diff[:min(len(set_B - set_A), num_ret)]

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
