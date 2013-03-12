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
tracker_address = "http://localhost:8080/req/"
num_of_caches = 2

class P2PUser():

    def __init__(self, tracker_ip, packet_size=2504):
        """ Create a new P2PUser.  Set the packet size, instantiate the manager,
        and establish clients.  Currently, the clients are static but will
        become dynamic when the tracker is implemented.
        """
        self.packet_size = packet_size
        my_ip = 'user'
        my_port = 30
        register_to_tracker_as_user(tracker_address, my_ip, my_port)
        # Connect to the server
        server_ip_address = retrieve_server_address_from_tracker(tracker_address)
        self.server_client = ThreadClient(server_ip_address, self.packet_size)
        # Connect to the caches
        cache_ip_addr = retrieve_caches_address_from_tracker(tracker_address, num_of_caches)
        print '[user.py] ' , cache_ip_addr
        self.tracker_ip = tracker_ip
        # Connect to the server
        # Cache will get a response when each chunk is downloaded from the server.
        # Note that this flag should **NOT** be set for the caches, as the caches
        # downloads will be aborted after 8 seconds with no expectation.
        # After the cache download period, the files themselves will be checked
        # to see what remains to be downloaded from the server.
        self.server_client.set_respond_RETR(True)

        self.server_client = ThreadClient(server_ip_address, self.packet_size)
        # Connect to the caches
        self.clients = []
        for i in xrange(len(cache_ip_addr)):
            self.clients.append(ThreadClient(cache_ip_addr[i], self.packet_size, i))
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
        inst_INTL = 'INTL ' + 'CNKN ' + vlen_items[2] # chunk size of typical frame (not last one)
        for i in range(len(self.clients)):
            client = self.clients[i]
            client.put_instruction(inst_INTL)
        self.server_client.put_instruction(inst_INTL)

        base_file = open('video-' + video_name + '/' + base_file_name, 'ab')
        for frame_number in xrange(start_frame, num_frames + 1):
            if frame_number == num_frames: # This is the last frame, so change chunk_size.
                inst_INTL = 'INTL ' + 'CNKN ' + vlen_items[3] # chunk size of last frame
                for i in range(len(self.clients)):
                    client = self.clients[i]
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
            if (num_chunks_rx_predicted >= 20):
                print "[user.py] 20 chunks assigned to caches; assume server will not be called."
            else:
                server_request = chunks_to_request(chosen_chunks, range(0, 40), 20 - num_chunks_rx_predicted)
                if server_request:
                    server_request_string = '%'.join(server_request)
                    # server should always be set with flag_deficit = 0 (has all chunks)
                    server_request_string = server_request_string + '&' + str(0)
                    self.server_client.put_instruction(inst_RETR + '.' + server_request_string)
                    if(DEBUGGING_MSG):
                        print "[user.py] Requesting from server: ", server_request
                elif (DEBUGGING_MSG):
                    print "No unique chunks from server requested."

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
            else:
                addtl_server_request = chunks_to_request(chunk_nums_rx, range(0, 40), 20 - num_chunks_rx)
                if addtl_server_request:
                    addtl_server_request_string = '%'.join(addtl_server_request)
                    # server should always be set with flag_deficit = 0 (has all chunks)
                    addtl_server_request_string = addtl_server_request_string + '&' + str(0)
                    self.server_client.put_instruction(inst_RETR + '.' + addtl_server_request_string)
                    if(DEBUGGING_MSG):
                        print "[user.py] Requesting from server: ", addtl_server_request
                elif (DEBUGGING_MSG):
                    print "No unique chunks from server requested."

            ###### WAIT FOR CHUNKS FROM SERVER TO FINISH DOWNLOADING: TIME 10 ######
            sleep(SERVER_DOWNLOAD_DURATION)

            if (DEBUGGING_MSG):
                print "[user.py] Waiting to receive all elements from server."
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
                # Open VLC Player
                os.system('/Applications/VLC.app/Contents/MacOS/VLC2 OnePiece575.flv &')

        for client in self.clients:
            client.put_instruction('QUIT')
        self.server_client.put_instruction('QUIT')
        print "[user.py] Closed all connections."

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

    set_A, set_B = sets.Set(str_A), sets.Set(str_B) # map all elts to str
    list_diff = list(set_B - set_A)
    list_diff.sort()
    return list_diff[:min(len(set_B - set_A), num_ret)]

def main():
    print "Arguments:", sys.argv

    packet_size_LUT = {'hyunah':194829, 'OnePiece575':169433}
    tracker_ip = 0

    if len(sys.argv) == 2:
        file_name = sys.argv[1]
        if file_name in packet_size_LUT:
            packet_size = packet_size_LUT[file_name]
        else:
            print '[user.py] The video ', file_name, ' does not exist.'
            sys.exit()
    else:
        sys.exit()

    packet_size = 0
    test_user = P2PUser(tracker_ip, packet_size)
    test_user.download(file_name, 1)

    # 'self' does not exist here
    for client in self.clients:
        client.put_instruction('QUIT')


if __name__ == "__main__":
    main()
