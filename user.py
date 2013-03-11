from streamer import *
from time import sleep
from helper import *
import os
import re
import sets
from threadclient import ThreadClient
from ftplib import error_perm
from zfec import filefec
import random

# Debugging MSG
DEBUGGING_MSG = True
VLC_PLAYER_USE = False

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
num_of_caches = 2
base_port = 60001
for i in range(num_of_caches):
    cache_ip_address.append((ip_local, base_port+i))
server_ip_address = (ip_local, 61000)

class P2PUser():

    def __init__(self, tracker_ip, packet_size=2504):
        """ Create a new P2PUser.  Set the packet size, instantiate the manager,
        and establish clients.  Currently, the clients are static but will
        become dynamic when the tracker is implemented.
        """
        self.packet_size = packet_size
        self.tracker_ip = tracker_ip
        # Connect to the server
        server_ip_address = get_server_address()
        self.server_client = ThreadClient(server_ip_address, self.packet_size)
        # Connect to the caches
        cache_ip_address = get_cache_addresses(2)
        cache_ip = cache_ip_address
        self.clients = []
        for i in xrange(len(cache_ip)):
            self.clients.append(ThreadClient(cache_ip[i], self.packet_size, i))
            # later: ask tracker.
        self.manager = None # TODO: create the manager class to decode/play

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
        try:
            os.mkdir('video-' + video_name)
        except:
            pass
        base_file = open('video-' + video_name + '/' + base_file_name, 'ab')
        for frame_number in xrange(start_frame, video_length):
            print '[user.py] frame_number : ', frame_number
            filename = 'file-' + video_name + '.' + str(frame_number)
            # directory for this frame
            folder_name = 'video-' + video_name + '/' + video_name + '.' + str(frame_number) + '.dir/'

            # get available chunks lists from cache A and B.
            inst_CNKS = 'CNKS ' + filename
            inst_RETR = 'RETR ' + filename

            available_chunks = [0]*len(self.clients) # available_chunks[i] = cache i's availble chunks
            rates = [0]*len(self.clients) # rates[i] = cache i's offered rate
            union_chunks = [] # union of all available indices
            for i in range(len(self.clients)):
                client = self.clients[i]
                client.put_instruction(inst_CNKS)
                return_str = client.get_response().split('&')
                if return_str[0] == '':
                    available_chunks[i] = []
                else:
                    available_chunks[i] = return_str[0].split('%')
                rates[i] = int(return_str[1])
                union_chunks = list( set(union_chunks) | set(available_chunks[i]) )

            print '[user.py]', available_chunks
            effective_rates = [0]*len(self.clients)
            assigned_chunks = [0]*len(self.clients)
            # index assignment here
            chosen_chunks = set([])
            for i in range(len(self.clients)):
                effective_available_chunks = list(set(available_chunks[i]) - chosen_chunks)
                effective_rates[i] = min(rates[i], len(effective_available_chunks))

                print '[user.py] eff_chunks', effective_available_chunks
                print '[user.py] eff_rate', effective_rates[i]
                assigned_chunks[i] = list(set(random.sample(effective_available_chunks, effective_rates[i])))
                # Temporarily convert str list to int list, sort it, convert it back
                print '[user.py]', assigned_chunks[i]
                int_assigned_chunks = map(int, assigned_chunks[i])
                int_assigned_chunks.sort()
                assigned_chunks[i] = map(str, int_assigned_chunks)
                chosen_chunks = (chosen_chunks | set(assigned_chunks[i]))

            flag_deficit = (sum(effective_rates) < 20) # True if user needs more rate from caches

            # request assigned chunks
            for i in range(len(self.clients)):
                client = self.clients[i]
                client_request_string = '%'.join(assigned_chunks[i])
                print "flag_deficit:", flag_deficit
                client_request_string = client_request_string + '&' + str(int(flag_deficit))
                print "Assigned chunks: ", assigned_chunks[i]
                print "Client request string: ", client_request_string
                client.put_instruction(inst_RETR + '.' + client_request_string)
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
                    # server should always be set with flag_deficit = 0 (has all chunks)
                    server_request_string = server_request_string + '&' + str(0)
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
                print 'size of base file:', os.path.getsize('video-' + video_name + '/' + base_file_name)
            print 'trying to decode'
            filefec.decode_from_files(base_file, chunksList)
            print 'decoded.  Size of base file =', os.path.getsize('video-' + video_name + '/' + base_file_name)
            if frame_number == 1 and VLC_PLAYER_USE:
                # Open VLC Player
                os.system('/Applications/VLC.app/Contents/MacOS/VLC2 OnePiece575.flv &')

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

def get_server_address():
    return server_ip_address

def get_cache_addresses(num_caches):
    return cache_ip_address

if __name__ == "__main__":
    print "Arguments:", sys.argv

    packet_size_LUT = {'hyunah':194829, 'OnePiece575':169433}

    if len(sys.argv) == 3:
        file_name = sys.argv[1]
        tracker_ip = sys.argv[2]
        if file_name in packet_size_LUT:
            packet_size = packet_size_LUT[file_name]
        else:
            print '[user.py] The video ', file_name, ' does not exist.'
            sys.exit()
    else:
        print '[user.py] user.py requires two arguments: filename and tracker IP.'
        sys.exit()

    test_user = P2PUser(tracker_ip, packet_size)
    test_user.download(file_name, 1)

    # 'self' does not exist here
    for client in self.clients:
        client.put_instruction('QUIT')
