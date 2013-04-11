from streamer import *
from time import sleep
from helper import *
import os
import re
from threadclient import ThreadClient
from ftplib import error_perm
from zfec import filefec
import random
import urllib2
import csv
import math
import sys

# Debugging MSG
DEBUGGING_MSG = True
VLC_PLAYER_USE = False

# Topology
USER_TOPOLOGY_UPDATE = True
T_choke = 3 # Choke period
T_choke2 = 2 # Choke period
eps_choke = 1 # Choke parameter

# Global parameters
CACHE_DOWNLOAD_DURATION = 8 # sec
SERVER_DOWNLOAD_DURATION = 2 # sec
DECODE_WAIT_DURATION = 0.1 # sec
tracker_address = load_tracker_address()
num_of_caches = 3

class P2PUser():

    #def __init__(self, tracker_address, video_name, packet_size):
    def __init__(self, tracker_address, video_name, user_name):
        """ Create a new P2PUser.  Set the packet size, instantiate the manager,
        and establish clients.  Currently, the clients are static but will
        become dynamic when the tracker is implemented.
        """
        self.packet_size = 1000
        self.user_name = user_name
        self.my_ip = user_name
        self.my_port = 0
        register_to_tracker_as_user(tracker_address, self.my_ip, self.my_port, video_name)

        # Connect to the server
        # Cache will get a response when each chunk is downloaded from the server.
        # Note that this flag should **NOT** be set for the caches, as the caches
        # downloads will be aborted after 8 seconds with no expectation.
        # After the cache download period, the files themselves will be checked
        # to see what remains to be downloaded from the server.
        server_ip_address = retrieve_server_address_from_tracker(tracker_address)
        self.server_client = ThreadClient(server_ip_address, self.packet_size)
        self.server_client.set_respond_RETR(True)
        self.tracker_address = tracker_address
        self.clients = []
        self.num_of_caches = num_of_caches
        self.manager = None # TODO: create the manager class to decode/play

    def VLC_start_video(self, video_path):
        # Put the file into the queue and play it
        url = 'http://127.0.0.1:8080/requests/status.xml?command=in_play&input=file://'
        url = url + video_path
        print '[user.py] ', url
        urllib2.urlopen(url).read()

    def VLC_pause_video(self):
        # Pause or play it
        url = 'http://127.0.0.1:8080/requests/status.xml?command=pl_pause'
        print '[user.py] ', url
        urllib2.urlopen(url).read()

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

    def connected_caches():
        f = open(config_file)
        fs = csv.reader(f, delimiter = ' ')
        for row in fs:
            if (DEBUGGING_MSG): print '[server.py] Loading movie : ', row
            movie_name = row[0]
            self.movies_LUT[movie_name] = (int(row[1]), int(row[2]), int(row[3]), int(row[4]), int(row[5]), int(row[6]))

    def download(self, video_name, start_frame):
        connected_caches = []
        not_connected_caches = []
        # Connect to the caches
        cache_ip_addr = retrieve_caches_address_from_tracker(self.tracker_address, 100, self.user_name)
	self.cache_ip_addr = cache_ip_addr
        #connected_caches = set([])
        self.num_of_caches = min(self.num_of_caches, len(cache_ip_addr))
        #connected_caches_index = [0] * self.num_of_caches
        #not_connected_caches = set(range(len(cache_ip_addr)))

        choke_state = 0 # 0 : usual state, 1 : overhead state
        choke_ct = 0

        for i in range(self.num_of_caches):
            each_client = ThreadClient(cache_ip_addr[i], self.packet_size, i)
            self.clients.append(each_client)
            connected_caches.append(each_client)
            print '[user.py] ', i, 'th connection is CONNECTED : ' , cache_ip_addr[i]

        for i in range(self.num_of_caches, len(cache_ip_addr)):
            each_client = ThreadClient(cache_ip_addr[i], self.packet_size, i)
            not_connected_caches.append(each_client)
            print '[user.py] ', i, 'th connection is RESERVED: ' , cache_ip_addr[i]

        available_chunks = set([])
        self.clients[0].put_instruction('VLEN file-%s' % (video_name))
        vlen_str = self.clients[0].get_response().split('\n')[0]
        vlen_items = vlen_str.split('&')
        print "VLEN: ", vlen_items
        num_frames = int(vlen_items[0])
        base_file_name = video_name + '.flv'
        try:
            os.mkdir('video-' + video_name)
        except:
            pass

        # Set internal chunk_size through putting an internal instruction into
        # the queue.
        base_file = open('video-' + video_name + '/' + base_file_name, 'ab')
        base_file_full_path = os.path.abspath('video-' + video_name + '/' + base_file_name)

        for frame_number in xrange(start_frame, num_frames + 1):
            sys.stdout.flush()
            effective_rates = [0]*len(self.clients)
            assigned_chunks = [0]*len(self.clients)

            if frame_number < num_frames: # Usual frames
                inst_INTL = 'INTL ' + 'CNKN ' + vlen_items[2] # chunk size of typical frame (not last one)
                for client in self.clients:
                    client.put_instruction(inst_INTL)
                self.server_client.put_instruction(inst_INTL)
            else: # Last frame
                inst_INTL = 'INTL ' + 'CNKN ' + vlen_items[3] # chunk size of last frame
                for client in self.clients:
                    client.put_instruction(inst_INTL)
                self.server_client.put_instruction(inst_INTL)

            print '[user.py] frame_number : ', frame_number
            filename = 'file-' + video_name + '.' + str(frame_number)
            # directory for this frame
            folder_name = 'video-' + video_name + '/' + video_name + '.' + str(frame_number) + '.dir/'

            # get available chunks lists from cache A and B.
            inst_CNKS = 'CNKS ' + filename
            inst_RETR = 'RETR ' + filename

            ###### DECIDING WHICH CHUNKS TO DOWNLOAD FROM CACHES: TIME 0 ######

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
                print '[user.py]', return_str[1]
                rates[i] = int(return_str[1])
                union_chunks = list( set(union_chunks) | set(available_chunks[i]) )

            print '[user.py]', available_chunks
            # index assignment here
            chosen_chunks = set([])
            sum_rate_so_far = 0
            for i in range(len(self.clients)):
                effective_available_chunks = list(set(available_chunks[i]) - chosen_chunks)
                effective_rates[i] = min(rates[i], len(effective_available_chunks), 20 - sum_rate_so_far)
                sum_rate_so_far = sum_rate_so_far + effective_rates[i]

                print '[user.py] CACHE ', i, 'eff_chunks ', effective_available_chunks
                print '[user.py] CACHE ', i, 'eff_rate', effective_rates[i]
                assigned_chunks[i] = list(set(random.sample(effective_available_chunks, effective_rates[i])))
                # Temporarily convert str list to int list, sort it, convert it back
                print '[user.py] CACHE ', i, 'assigned_chunks', assigned_chunks[i]
                int_assigned_chunks = map(int, assigned_chunks[i])
                int_assigned_chunks.sort()
                assigned_chunks[i] = map(str, int_assigned_chunks)
                chosen_chunks = (chosen_chunks | set(assigned_chunks[i]))

            flag_deficit = (sum(effective_rates) < 20) # True if user needs more rate from caches

            # request assigned chunks
            for i in range(len(self.clients)):
                client = self.clients[i]
                client_request_string = '%'.join(assigned_chunks[i])
                client_request_string = client_request_string + '&' + str(int(flag_deficit))
                print "[user.py] [Client " + str(i) + "] flag_deficit: ", int(flag_deficit), \
                    ", Assigned chunks: ", assigned_chunks[i], \
                    ", Request string: ", client_request_string
                client.put_instruction(inst_RETR + '.' + client_request_string)

            ###### DECIDING CHUNKS THAT HAVE TO BE DOWNLOADED FROM CACHE: TIME 0 ######
            # Before CACHE_DOWNLOAD_DURATION, also start requesting chunks from server.
            server_request = []
            chosen_chunks = list(chosen_chunks)
            num_chunks_rx_predicted = len(chosen_chunks)
            if True:
                server_request = chunks_to_request(chosen_chunks, range(0, 40), 20 - num_chunks_rx_predicted)
                server_request_string = '%'.join(server_request)
                server_request_string = server_request_string + '&' + str(1) ## DOWNLOAD FROM SERVER : binary_g = 1
                self.server_client.put_instruction(inst_RETR + '.' + server_request_string)
            else:
                if (num_chunks_rx_predicted >= 20):
                    print "[user.py] 20 chunks assigned to caches; assume server will not be called."
                else:
                    server_request = chunks_to_request(chosen_chunks, range(0, 40), 20 - num_chunks_rx_predicted)
                    if server_request:
                        server_request_string = '%'.join(server_request)
                        # server should always be set with flag_deficit = 0 (has all chunks)
                        server_request_string = server_request_string + '&' + str(1) ## DOWNLOAD FROM SERVER : binary_g = 1
                        self.server_client.put_instruction(inst_RETR + '.' + server_request_string)
                        if(DEBUGGING_MSG):
                            print "[user.py] Requesting from server: ", server_request
                    elif (DEBUGGING_MSG):
                        print "No unique chunks from server requested."

            num_of_chks_from_server = len(server_request)
            #update_server_load(tracker_address, video_name, num_of_chks_from_server)

            sleep(CACHE_DOWNLOAD_DURATION)
            ###### STOPPING CACHE DOWNLOADS: TIME 8 (CACHE_DOWNLOAD_DURATION) ######

            # immediately stop cache downloads.
            for client in self.clients:
                client.client.abort()
            print "[user.py] Cache connections aborted for frame %d" % (frame_number)

            ###### REQUEST ADDITIONAL CHUNKS FROM SERVER: TIME 8 (CACHE_DOWNLOAD_DURATION) ######
            # Request from server remaining chunks missing
            # Look up the download directory and count the downloaded chunks
            chunk_nums_rx = chunk_nums_in_frame_dir(folder_name)
            if (DEBUGGING_MSG):
                print "%d chunks received so far for frame %d: " % (len(chunk_nums_rx), frame_number)
                print chunk_nums_rx

            # Add the chunks that have already been requested from server

            chunk_nums_rx = list (set(chunk_nums_in_frame_dir(folder_name)) | set(server_request))
            addtl_server_request = []
            num_chunks_rx = len(chunk_nums_rx)
            if (num_chunks_rx >= 20):
                print "[user.py] No additional chunks to download from the server."
                print '[user.py] Sending an empty RETR'
                self.server_client.put_instruction(inst_RETR + '.&1')
            else:
                addtl_server_request = chunks_to_request(chunk_nums_rx, range(0, 40), 20 - num_chunks_rx)
                if addtl_server_request:
                    addtl_server_request_string = '%'.join(addtl_server_request)
                    # server should always be set with flag_deficit = 0 (has all chunks)
                    addtl_server_request_string = addtl_server_request_string + '&' + str(1) ## DOWNLOAD FROM SERVER : binary_g = 1
                    self.server_client.put_instruction(inst_RETR + '.' + addtl_server_request_string)
                    if(DEBUGGING_MSG):
                        print "[user.py] Requesting from server: ", addtl_server_request
                elif (DEBUGGING_MSG):
                    print "No unique chunks from server requested."

            ###### WAIT FOR CHUNKS FROM SERVER TO FINISH DOWNLOADING: TIME 10 ######
            sleep(SERVER_DOWNLOAD_DURATION)

            if (DEBUGGING_MSG):
                print "[user.py] Waiting to receive all elements from server."
            if frame_number > start_frame and (server_request or addtl_server_request) and VLC_PLAYER_USE:
                # Need to pause it!
                self.VLC_pause_video()
            if server_request:
                resp_RETR = self.server_client.get_response()
                parsed_form = parse_chunks(resp_RETR)
                fname, framenum, binary_g, chunks = parsed_form
                print "[user.py] Downloaded chunks from server: ", chunks
            if addtl_server_request:
                resp_RETR = self.server_client.get_response()
                parsed_form = parse_chunks(resp_RETR)
                fname, framenum, binary_g, chunks = parsed_form
                print "[user.py] Downloaded chunks from server: ", chunks

            # Now play it
            if frame_number > start_frame and (server_request or addtl_server_request) and VLC_PLAYER_USE:
                self.VLC_pause_video()

            chunk_nums = chunk_nums_in_frame_dir(folder_name)
            num_chunks_rx = len(chunk_nums)
            if num_chunks_rx >= 20 and DEBUGGING_MSG:
                print "[user.py] Received 20 packets"
            else:
                print "[user.py] Did not receive 20 packets for this frame."

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
                self.VLC_start_video(base_file_full_path)

            if USER_TOPOLOGY_UPDATE:
                if choke_state == 0: # Normal state
                    print '[user.py] Normal state : ', choke_ct
                    choke_ct += 1
                    if choke_ct == T_choke:
                        choke_ct = 0
                        if len(not_connected_caches) == 0:
                            pass
                        else: # Add a new cache temporarily
                            new_cache_index = random.sample(range(len(not_connected_caches)), 1)
                            if new_cache_index >= 0:
                                new_cache = not_connected_caches[new_cache_index[0]]
                                self.clients.append(new_cache)
                                connected_caches.append(new_cache)
                                not_connected_caches.remove(new_cache)
                                print '[user.py] Topology Update : Temporarily added ', new_cache.address
                                choke_state = 1 # Now, move to transitional state
                                choke_ct = 0
                                print '[user.py] Topology Update : Now the state is changed to overhead staet'
				print '[user.py]', connected_caches, not_connected_caches, self.clients	

                elif choke_state == 1: # Overhead state
                    print '[user.py] Overhead state : ', choke_ct
                    choke_ct += 1
                    if choke_ct == T_choke2: # Temporary period to spend with temporarily added node
                        rate_vector = [0] * len(self.clients)
                        p_vector = [0] * len(self.clients)
                        for i in range(len(self.clients)):
                            rate_vector[i] = len(assigned_chunks[i])
                            p_vector[i] = math.exp( -eps_choke * rate_vector[i])

# >>> cdf = [(1, 0), (2,0.1), (3,0.15), (4,0.2), (5,0.4), (6,0.8)]
                        p_sum = sum(p_vector)
                        for i in range(len(self.clients)):
                            p_vector[i] /= p_sum

                        cdf = [(0,0)] * len(self.clients)
                        cdf[0] = (0, 0)
                        for i in range(1, len(self.clients)):
                            cdf[i] = (i, cdf[i-1][1] + p_vector[i-1])

                        print '[user.py] cdf :', cdf
                        client_index = max(i for r in [random.random()] for i,c in cdf if c <= r) # http://stackoverflow.com/questions/4265988/generate-random-numbers-with-a-given-numerical-distribution
                        # client_index = rate_vector.index(min(rate_vector))

                        removed_cache = self.clients[client_index]
            		removed_cache.put_instruction('QUIT')
                        self.clients.remove(removed_cache)
                        connected_caches.remove(removed_cache)
            		new_cache = ThreadClient(self.cache_ip_addr[i], 1000, client_index)
                        not_connected_caches.append(new_cache)

                        print '[user.py] Topology Update : ', removed_cache, 'is chocked.'

                        choke_state = 0 # Now, move to normal state
                        choke_ct = 0

    def disconnect(self, tracker_address, video_name, user_name):
        for client in self.clients:
            client.put_instruction('QUIT')
        self.server_client.put_instruction('QUIT')
        print "[user.py] Closed all connections."

        my_ip = user_name
        my_port = 0
        my_video_name = video_name
        deregister_to_tracker_as_user(tracker_address, my_ip, my_port, video_name)
        print "[user.py] BYE"


def chunks_to_request(A, B, num_ret):
    """ Find the elements in B that are not in A. From these elements, return a
    randomized set that has maximum num_ret elements.

    Example: A = {1, 3, 5, 7, 9}, B = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14},
    num_ret = 5 possible element sets: {2, 4, 6, 8, 10}, {2, 4, 6, 8, 12}, and
    so on.

    For now, it may just be easiest to take the first num_ret elements of the
    non-overlapping set instead of randomizing the elements to choose from the
    non-overlapping set. """

    str_A = map(str, A)
    str_B = map(str, B)

    for i in range(len(str_A)):
        str_A[i] = str_A[i].zfill(2)
    for i in range(len(str_B)):
        str_B[i] = str_B[i].zfill(2)
    #print str_A
    #print str_B

    set_A, set_B = set(str_A), set(str_B) # map all elts to str
    list_diff = list(set_B - set_A)
    list_diff.sort()
    return list_diff[:min(len(set_B - set_A), num_ret)]

def main():
    print "Arguments:", sys.argv
    #test_user = P2PUser(tracker_address, video_name, packet_size)
    video_name = sys.argv[1]
    user_name = sys.argv[2]
    print '[user.py]', tracker_address
    test_user = P2PUser(tracker_address, video_name, user_name)
    test_user.download(video_name, 1)
    test_user.disconnect(tracker_address, video_name, user_name)

if __name__ == "__main__":
    main()
