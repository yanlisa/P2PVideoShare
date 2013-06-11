from streamer import StreamFTP
from threadclient import *
from server import *
from pyftpdlib import ftpserver
import Queue
import random
import csv
import time
import threading
import resource
import sys
from helper import *

# Debugging MSG
DEBUGGING_MSG = False

# Algorithm DEBUGGING
POSITIVE_CONSTRAINT = True

# Cache Configuration
cache_config_file = '../../config/cache_config.csv'

# Log Configuration
LOG_PERIOD = 1000

INFINITY = 10e10
MAX_CONNS = 1000
MAX_VIDEOS = 1000
BUFFER_LENGTH = 10
path = "."
tracker_address = load_tracker_address() # set in helper.

# CACHE RESOURCE
#BANDWIDTH_CAP = 2000 # (Kbps)
BANDWIDTH_CAP = 8000 * 3# (Kbps)
STORAGE_CAP_IN_MB = 60 * 3 # (MB)
#STORAGE_CAP_IN_MB = 45 # (MB)

T_rate = .01
T_storage = .01
#T_rate = .1
#T_storage = .1
T_topology = 600
STORAGE_UPDATE_PERIOD_OUTER = 1

# IP Table
class ThreadStreamFTPServer(StreamFTPServer, threading.Thread):
    """
        A threaded server. Requires a Handler.
    """
    def __init__(self, address, handler, spec_rate=0):
        StreamFTPServer.__init__(self, address, handler, spec_rate)
        threading.Thread.__init__(self)

    def run(self):
        self.serve_forever()

    def get_conns(self):
        return self.conns

    def get_handlers(self):
        #print '[cache.py] self.handlers = ', self.handlers
        return self.handlers

class Cache(object):
    """
    Manages the cache as a whole. Has 3 main functions:
    -Opens up an FTP mini-server (CacheServer) in one thread.
    -Opens up a connection to the actual server (ServerDownloader) in a thread.
    -Every <timestep> check to see if cache is properly serving user-needs.
    If not, use ServerDownloader to request different chunks from the server.
    """

    def __init__(self, cache_config):
        """Make the FTP server instance and the Cache Downloader instance.
        Obtain the queue of data from the FTP server."""

        # Parse cache configuration
        # [ cache_id,
        #   private_ip, port #,
        #   public_ip,
        #   stream_rate,
        #   num_of_chunks_cache_stores ]
        self.packet_size = 2504
        server_ip_address = get_server_address(tracker_address)
        self.server_client = ThreadClient(server_ip_address, self.packet_size)
        # Cache will get a response when each chunk is downloaded.
        self.server_client.set_respond_RETR(True)

        cache_id = int(cache_config[0])
        self.address = (cache_config[1], int(cache_config[2]))
        self.public_address = cache_config[3]
        register_to_tracker_as_cache(tracker_address, self.public_address, self.address[1])
        # register_to_tracker_as_cache(tracker_address, self.address[0], self.address[1])
        # print '[cache.py] Address : ', (self.public_address, self.address[1])
        stream_rate = int(cache_config[4])
        # num_of_chunks_cache_stores = int(cache_config[5])

        if True:
            # Variables for algorithms
            average_streaming_rate = 3000 # Kbps
            average_length = 120 # sec

            scale = 3
            self.eps_x = 1 * scale
            self.eps_k = 1 * scale
            self.eps_la = .3 * scale
            self.eps_f = .001 * scale
            self.eps_mu = .0000001 * scale

            print '[cache.py] EPS'
            print '[cache.py] eps_x ', self.eps_x
            print '[cache.py] eps_f ', self.eps_f
            print '[cache.py] eps_k ', self.eps_k
            print '[cache.py] eps_la ', self.eps_la
            print '[cache.py] eps_mu ', self.eps_mu
        else:
            # Variables for algorithms
            eps = .01
            average_streaming_rate = 3000 # Kbps
            average_length = 120 # sec
            self.eps_x = eps * 100
            self.eps_f = eps / 500 / 8
            self.eps_la = eps / 100
            self.eps_mu = eps / pow(average_length, 2) / 10
            self.eps_k = eps

        self.bandwidth_cap = BANDWIDTH_CAP # (Kbps)
        self.storage_cap_in_MB = STORAGE_CAP_IN_MB # (MB)
        self.storage_cap = self.storage_cap_in_MB * 1000 * 1000 # (Bytes)

        self.dual_la = 0 # dual variable for BW
        self.dual_mu = 0 # dual variable for ST

        self.primal_x = [0] * MAX_CONNS
        self.primal_f = {}
        self.dual_k = [0] * MAX_CONNS

        self.sum_rate = 0
        self.sum_storage = 0

        self.authorizer = ftpserver.DummyAuthorizer()
        # allow anonymous login.
        self.authorizer.add_anonymous(path, perm='elr')
        handler = CacheHandler
        handler.parentCache = self
        # handler.timeout = 1
        handler.authorizer = self.authorizer
        self.movie_LUT = retrieve_MovieLUT_from_tracker(tracker_address)
        handler.movie_LUT = self.movie_LUT
        handler.passive_ports = range(60000, 65535)

        # set public.
        handler.masquerade_address = self.public_address
        handler.id_to_index = {} # Dictionary from ID to index
        handler.rates = [0]*MAX_CONNS # in chunks per frame
        # handler.chunks = [chunks]*MAX_VIDEOS
        handler.chunks = {}
        handler.binary_g = [0]*MAX_CONNS # binary: provided chunks sufficient for conn
        handler.watching_video = ['']*MAX_CONNS

        # Create server on this cache.
        self.mini_server = ThreadStreamFTPServer(self.address, handler, stream_rate)
        print "Cache streaming rate set to ", self.mini_server.stream_rate
        handler.set_movies_path(path)

    def start_cache(self):
        """Start the FTP server and the CacheDownloader instance.
        Every <timestamp>, obtain the recorded data from the FTP server queue
        and ask the server for additional chunks if needed."""
        print 'Trying to run a cache...'
        self.mini_server.start()
        self.start_control()
        print 'Cache is running...'

    def get_conns(self):
        return self.mini_server.get_conns()

    def get_handlers(self):
        return self.mini_server.get_handlers()

    def set_conn_rate(self, index, new_rate):
        self.mini_server.get_handlers()[index].set_packet_rate(new_rate)

    def get_conn_rate(self, index):
        return CacheHandler.rates[index]

    def get_chunks(self, video_name):
        if video_name in CacheHandler.chunks.keys():
            return CacheHandler.chunks[video_name]
        else:
            return []

    def set_chunks(self, video_name, new_chunks):
        # print '[cache.py]', new_chunks
        CacheHandler.chunks[video_name] = new_chunks
        # self.mini_server.get_handlers()[index].set_chunks(new_chunks)

    def get_g(self, index):
        return CacheHandler.binary_g[index]

    def get_watching_video(self, index):
        return CacheHandler.watching_video[index]

    def bound(self, h, y, a, b):
       # if (a > b)
       #     Illegal bound specified, a(lower bound) must be less than or equal to b(upper bound
       # end
       if y >= b:
           return min(0, h)
       elif y <= a:
           return max(0, h)
       else:
           return h

    def rate_update(self, T_period):
        self.rate_update_optimal(T_period)
        #self.rate_udpate_greedy(T_period)

    def rate_update_optimal(self, T_period):
        log_ct = 0
        while True:
            log_ct = log_ct + 1
            if log_ct == LOG_PERIOD:
                log_ct = 0
            time.sleep(T_period)
            if DEBUGGING_MSG:
                print '[cache.py] RATE ALLOCATION BEGINS'
            handlers = self.get_handlers()
            if len(handlers) == 0:
                if DEBUGGING_MSG:
                    print '[cache.py] No user is connected'
            else:
                sum_x = 0
                print '[cache.py] update PRIMAL_X'
                for i in range(len(handlers)):
                    if i not in CacheHandler.id_to_index.values():
                        # print '[cache.py]', i, 'is not in map values, we skip'
                        continue

                    ## 1. UPDATE PRIMAL_X
                    handler = handlers[i]
                    print '[cache.py] ' + str(i) + 'th connection, index = ' + str(handler.index)
                    if handler._closed == True:
                        if DEBUGGING_MSG:
                            print '[cache.py] Connection ' + str(i) + ' is closed'
                        continue
                    else:
                        if DEBUGGING_MSG:
                            print '[cache.py] Connection ' + str(i) + ' is open'
                    video_name = self.get_watching_video(i)
                    code_param_n = self.movie_LUT.code_param_n_lookup(video_name)
                    code_param_k = self.movie_LUT.code_param_k_lookup(video_name)
                    if DEBUGGING_MSG:
                        print '[cache.py] User ' + str(i) + ' is watching ' + str(video_name)
                    packet_size = self.movie_LUT.chunk_size_lookup(video_name)
                    if packet_size == 0:
                        continue

                    # First, find the optiaml variables
                    g = self.get_g(i)
                    print "[cache.py] Connection " + str(i) + " : (self.index, g, x) ", (handlers[i].index,  g, self.primal_x[i])
                    print "[cache.py] Connection " + str(i) + " : (g, self.dual_la, self.dual_k[i]) ", (g, self.dual_la, self.dual_k[i])
                    delta_x = self.bound(g - (self.dual_la + self.dual_k[i]), \
                                        self.primal_x[i], 0, self.bandwidth_cap)
                    self.primal_x[i] += self.eps_x * delta_x
                    if POSITIVE_CONSTRAINT:
                        self.primal_x[i] = max(self.primal_x[i], 0)
                    sum_x += self.primal_x[i]

                    # Apply it if it goes over some rate
                    rate_per_chunk = packet_size / 1000 / BUFFER_LENGTH * 8 # (Kbps)
                    assigned_rate = max(round(self.primal_x[i] / rate_per_chunk), 0)
                    num_of_stored_chunks = len(self.get_chunks(video_name)) # FIX : it should be video(i)
                    self.set_conn_rate(i, min(assigned_rate, num_of_stored_chunks))
                    #current_rate = self.get_conn_rate(i)
                    if log_ct == 0:
                        print '[cache.py] User ' + str(i) + ' (g,x,assigned_rate,num_of_stored) ' + str(g) + ',' + str(self.primal_x[i]) + ',' + str(assigned_rate) + ',' + str(num_of_stored_chunks)

                    ## 2. UPDATE DUAL_K
                    if video_name not in self.primal_f.keys():
                        self.primal_f[video_name] = 0.0
                    if DEBUGGING_MSG:
                        print '[cache.py] total rate = ', (rate_per_chunk * code_param_k)
                    delta_k = self.bound(self.primal_x[i] - self.primal_f[video_name] * rate_per_chunk * code_param_k, self.dual_k[i], 0, INFINITY)
                    if log_ct == 0:
		                print '[cache.py] User ' + str(i) + ' delta_k ' + str(delta_k)
                    self.dual_k[i] += self.eps_k * delta_k
                    if POSITIVE_CONSTRAINT:
                        self.dual_k[i] = max(0, self.dual_k[i])
                    if log_ct == 0:
                        print '[cache.py] User ' + str(i) + ' dual_k ' + str(self.dual_k[i])

                ## 3. UPDATE DUAL_LA
                if log_ct == 0:
		            print '[cache.py] sum_x ' , sum_x
                delta_la = self.bound(sum_x - self.bandwidth_cap, self.dual_la, 0, INFINITY)
                self.dual_la += self.eps_la * delta_la
                if POSITIVE_CONSTRAINT:
                    self.dual_la = max(self.dual_la, 0)
                if log_ct == 0:
                    print '[cache.py] dual_la ' + str(self.dual_la)

    def remove_one_chunk(self, video_name, index):
        # It should remove all the downloaded chunks at cache
        # Currently, it just removes the index out of the cache_chunk_list

        return True

    def download_one_chunk_from_server(self, video_name, index):
        print '[cache.py] Caching chunk', index , 'of' , video_name
        packet_size = self.movie_LUT.chunk_size_lookup(video_name)
        frame_num = self.movie_LUT.frame_num_lookup(video_name)
        if packet_size == 0: # This must not happen.
            return True

        last_packet_size = self.movie_LUT.last_chunk_size_lookup(video_name)
        if last_packet_size == 0:
            return True
        inst_INTL = 'INTL ' + 'CNKN ' + str(packet_size) # chunk size of typical frame (not last one)
        inst_INTL_LAST = 'INTL ' + 'CNKN ' + str(last_packet_size) # chunk size of last frame

        chosen_chunks = index
        server_request = map(str, chosen_chunks)
        server_request_string = '%'.join(server_request) + '&0' # The last digit '0' means 'I am cache'

        self.server_client.put_instruction(inst_INTL) # set chunk_size to typical frame.
        for i in range(1, frame_num+1):
            if i == frame_num: # This is the last frame, so change chunk_size.
                self.server_client.put_instruction(inst_INTL_LAST)
            filename = 'file-' + video_name + '.' + str(i)
            inst_RETR = 'RETR ' + filename
            self.server_client.put_instruction(inst_RETR + '.' + server_request_string)
            # wait till download is completed
            resp_RETR = self.server_client.get_response()
            parsed_form = parse_chunks(resp_RETR)
            fname, framenum, chunks, user_or_cache = parsed_form
            print '[cache.py] Finished downloading: Frame %s Chunk %s' % (framenum, chunks)
        return True

    def storage_update(self, T_period):
        self.storage_update_optimal(T_period)
        # self.storage_update_greedy(T_period)

    def storage_update_optimal(self, T_period):
        ct = 0
        log_ct = 0
        while True:
            time.sleep(T_period)

            ct += 1
            log_ct += 1
            if log_ct == LOG_PERIOD:
                log_ct = 0

            if DEBUGGING_MSG:
                print '[cache.py] STORAGE ALLOCATION BEGINS'
            handlers = self.get_handlers()
            if len(handlers) == 0:
                if DEBUGGING_MSG:
                    print '[cache.py] No user is connected'
            else:
                sum_storage_virtual = 0
                video_check_list = {}

                ## 1. UPDATE PRIMAL_F
                # print '[cache.py] Update dual_k'
                for i in range(len(handlers)):

                    if i not in CacheHandler.id_to_index.values():
                        print '[cache.py]', i, 'is not in map values, we skip'
                        continue

                    handler = handlers[i]
                    print '[cache.py] ' + str(i) + 'th connection, index = ' + str(handler.index)
                    if handler._closed == True:
                        if DEBUGGING_MSG:
                            print '[cache.py] Connection ' + str(i) + ' is closed'
                        continue
                    else:
                        if DEBUGGING_MSG:
                            print '[cache.py] Connection ' + str(i) + ' is open'

                    # Open connection
                    current_rate = self.get_conn_rate(i)
                    video_name = self.get_watching_video(i)
                    if video_name in video_check_list.keys():
                        continue
                    else:
                        video_check_list[video_name] = True
                        if log_ct == 0:
                            print '[cache.py] Updating primal_f for video', video_name
                    code_param_n = self.movie_LUT.code_param_n_lookup(video_name)
                    code_param_k = self.movie_LUT.code_param_k_lookup(video_name)
                    packet_size = self.movie_LUT.chunk_size_lookup(video_name)
                    frame_num = self.movie_LUT.frame_num_lookup(video_name)
                    additional_storage_needed = packet_size * frame_num # Rough
                    if packet_size == 0:
                        continue
                    dual_k_sum = 0

                    tmp_sum = 0
                    for j in range(len(handlers)):
                        # print '[cache.py]DEBUG__ dual_k[', j, '] =', self.dual_k[j]
                        tmp_sum = self.dual_k[j]
                    # print '[cache.py]DEBUG__ dual_k sum =', tmp_sum
                    # print '[cache.py]DEBUG__ self.dual_k', self.dual_k[:2]

                    for j in range(len(handlers)):
                        handler_j = handlers[j]
                        if handler_j._closed == True:
                            continue
                        if self.get_watching_video(j) == video_name:
                            dual_k_sum += self.dual_k[j]
                    if log_ct == 0:
                        print '[cache.py] dual_k_sum = ', dual_k_sum
                        print '[cache.py] dual_mu = ', self.dual_mu
                    if video_name not in self.primal_f.keys():
                        self.primal_f[video_name] = 0.0
                    delta_f = self.bound(dual_k_sum - frame_num * BUFFER_LENGTH * self.dual_mu, self.primal_f[video_name], 0, 1)
                    self.primal_f[video_name] += self.eps_f * delta_f
                    self.primal_f[video_name] = max(self.primal_f[video_name], 0)
                    sum_storage_virtual += self.primal_f[video_name] * self.movie_LUT.size_bytes_lookup(video_name)

                    if log_ct == 0:
                        print '[cache.py] primal_f[' + video_name + '] = ' + str(self.primal_f[video_name])
                    if DEBUGGING_MSG:
                        if log_ct == 0:
                            print '[cache.py] delta_f = %.5f' % delta_f
                            print '[cache.py] self.eps_f = %.5f' % self.eps_f
                            print '[cache.py] self.eps_f * delta_f = %.5f ' % (self.eps_f * delta_f)

                    stored_chunks = self.get_chunks(video_name)
                    if log_ct == 0:
                        print '[cache.py] stored_chunks ', stored_chunks
                    num_stored_chunks = len(stored_chunks)
                    assigned_num_of_chunks = min(max(int(self.primal_f[video_name] * code_param_k), 0), code_param_k) # ceiling
                    if log_ct == 0:
                        print '[cache.py] num_stored_chunks ', num_stored_chunks
                        print '[cache.py] assigned_num_of_chks ', assigned_num_of_chunks
                    if ct % STORAGE_UPDATE_PERIOD_OUTER == 0:
                        if assigned_num_of_chunks > num_stored_chunks:
                            if num_stored_chunks >= code_param_k:
                                if log_ct == 0:
                                    print '[cache.py] Downloading nothing from server'
                                print '[cache.py] Logic error'
                                sys.exit(0)
                                #self.download_one_chunk_from_server(video_name, '')
                            else:
                                chunk_index = random.sample( list(set(range(0,code_param_n)) - set(map(int, stored_chunks))), 1 ) # Sample one out of missing chunks
                                if self.download_one_chunk_from_server(video_name, chunk_index) == True:
                                    new_chunks = list(set(self.get_chunks(video_name)) | set(map(str, chunk_index)))
                                    self.set_chunks(video_name, new_chunks)
                                    update_chunks_for_cache(tracker_address, self.public_address, self.address[1], video_name, new_chunks)
                                    self.sum_storage = self.sum_storage + additional_storage_needed
                                    if log_ct == 0:
                                        print '[cache.py] chunk add done'
                                        print '[cache.py] storage Usage' , int(self.sum_storage/1000/1000) , '(MB) /' , int(self.storage_cap/1000/1000) , '(MB)'
                        elif assigned_num_of_chunks < num_stored_chunks:
                            if len(stored_chunks) == 0:
                                pass
                            else:
                                chunk_index = random.sample( list(set(stored_chunks)), 1 )
                                if self.remove_one_chunk(video_name, chunk_index) == True:
                                    new_chunks = list(set(self.get_chunks(video_name)) - set(map(str, chunk_index)))
                                    self.set_chunks(video_name, new_chunks)
                                    update_chunks_for_cache(tracker_address, self.public_address, self.address[1], video_name, new_chunks)
                                    self.sum_storage = self.sum_storage - additional_storage_needed
                                    if log_ct == 0:
                                        print '[cache.py] chunk ', chunk_index, ' is dropped'
                                        print '[cache.py] storage Usage' , int(self.sum_storage/1000/1000) , '(MB) /' , int(self.storage_cap/1000/1000) , '(MB)'
                        else:
                            if log_ct == 0:
                                print '[cache.py] storage not updated'

                ## 2. UPDATE DUAL_K
                # print '[cache.py] Update dual_k'
                for i in range(len(handlers)):
                    if i not in CacheHandler.id_to_index.values():
                        # print '[cache.py]', i, 'is not in map values, we skip'
                        continue

                    handler = handlers[i]
                    if handler._closed == True:
                        if log_ct == 0:
                            print '[cache.py] Connection ' + str(i) + ' is closed'
                        continue
                    video_name = self.get_watching_video(i)
                    if video_name not in self.primal_f.keys():
                        self.primal_f[video_name] = 0.0
                    packet_size = self.movie_LUT.chunk_size_lookup(video_name)
                    if packet_size == 0:
                        continue
                    rate_per_chunk = packet_size / 1000 / BUFFER_LENGTH * 8 # (Kbps)
                    if log_ct == 0:
                        print '[cache.py] self.primal_f', self.primal_f
                        print '[cache.py] self.primal_f[', video_name, '] = ', self.primal_f[video_name]
                    delta_k = self.bound(self.primal_x[i] - self.primal_f[video_name] * rate_per_chunk * code_param_k, self.dual_k[i], 0, INFINITY)
                    if log_ct == 0:
                        print '[cache.py] User ' + str(i) + ' delta_k ' + str(delta_k)
                    self.dual_k[i] += self.eps_k * delta_k
                    if POSITIVE_CONSTRAINT:
                        self.dual_k[i] = max(0, self.dual_k[i])
                    if log_ct == 0:
                        print '[cache.py] User ' + str(i) + ' dual_k ' + str(self.dual_k[i])
                    # print '[cache.py]DEBUG__ self.dual_k', self.dual_k[:2]

                # Need to update dual_mu
                if log_ct == 0:
                    print '[cache.py] self.sum_storage ', self.sum_storage
                    print '[cache.py] self.sum_storage_virtual ', sum_storage_virtual
                delta_mu = self.bound(sum_storage_virtual - self.storage_cap, self.dual_mu, 0, INFINITY)
                self.dual_mu += self.eps_mu * delta_mu
                if POSITIVE_CONSTRAINT:
                    self.dual_mu = max(self.dual_mu, 0)
                if log_ct == 0:
                    print '[cache.py] dual_mu ' + str(self.dual_mu)

    def topology_update(self, T_period):
        while True:
            print '[cache.py] topology updating'
            time.sleep(T_period)

    def connection_check(self):
        pass
        # print '[cache.py] connection checking'
        #conns = self.get_conns()

        # Currently assuming 'a single movie'. It needs to be generalized
        #for i in range(len(conns)):
        #CacheHandler.connected[self.index] = True

    def start_control(self):
        th1 = threading.Thread(target=self.rate_update, args=(T_rate,))
        th2 = threading.Thread(target=self.storage_update, args=(T_storage,))
        th3 = threading.Thread(target=self.topology_update, args=(T_topology,))
        threads = [th1, th2, th3]

        for th in threads:
            th.start()

###### HANDLER FOR EACH CONNECTION TO THIS CACHE######

class CacheHandler(StreamHandler):
    """
    The mini-server handler that serves users on this address.

    The mini-server only stores a particular set of chunks per frame,
    and that set will be what it sends to the user per frame requested.
    """
    chunks = []
    rates = []
    watching_video = []
    connected = []
    stream_rate = 10*1024 # Default is 10 Kbps
    def __init__(self, conn, server, index=0, spec_rate=0):
        print '[cache.py]', index
        super(CacheHandler, self).__init__(conn, server, index, spec_rate)

    def close(self): # Callback function on a connection close
        print '[cache.py] connection is closed'
        StreamHandler.close(self)

    def set_chunks(self, new_chunks):
        """
        Adjusts the set of chunks that this cache holds across all frames.
        """
        CacheHandler.chunks[self.index] = new_chunks

    def set_binary_g(self, new_g):
        """
        Adjusts the binary value of the user's satisfaction with this connection.
        """
        CacheHandler.binary_g[self.index] = new_g

    def set_packet_rate(self, new_rate):
        CacheHandler.rates[self.index] = new_rate
#        print "Packet rate has been set within CacheHandler for: %d conn to rate %d" % (self.index, new_rate)

    def ftp_CNKS(self, arg):
        """
        FTP command: Returns this cache's chunk number set.
        """
        # print "index:", self.index
        # print '[cache.py] CacheHandler.chunks', CacheHandler.chunks
        # print '[cache.py] line', arg
        video_name = arg.split('file-')[-1].split('.')[0]
        # print '[cache.py] video_name ', video_name
        if video_name in CacheHandler.chunks.keys():
            data = '%'.join(map(str, CacheHandler.chunks[video_name]))
        else:
            data = '%'.join(map(str, ''))
        data = data + '&' + str(int(CacheHandler.rates[self.index]))
        # print "Sending CNKS: ", data
        #CacheHandler.connected[self.index] = True
        self.push_dtp_data(data, isproducer=False, cmd="CNKS")

    def ftp_UPDG(self, line):
        """
        FTP command: Update g(satisfaction signal) from users.
        """
        # Update G for this user
        CacheHandler.binary_g[self.index] = int(line)
        self.respond("200 I successfully updated g=" + line + " for the user" + str(self.index))

    def ftp_ID(self, line):
        """
        FTP command: Update ID from users.
        """
        # line = ID
        # print "[cache.py] CacheHandler.id_to_index =", CacheHandler.id_to_index
        if line not in CacheHandler.id_to_index.keys():
            CacheHandler.id_to_index[line] = self.index # Data transfer conection
            # print "[cache.py] Successfully added (ID, index) = (" + line + ", " + str(self.index) + ")"
            self.respond("200 I successfully added (ID, index) = (" + line + ", " + str(self.index) + ")")
        else:
            self.index = CacheHandler.id_to_index[line] # Info transfer conection
            # print "[cache.py] Successfully matched a connection for (ID, index) = (" + line + ", " + str(self.index) + ")"
            self.respond("200 I successfully matched a connection for (ID, index) = (" + line + ", " + str(self.index) + ")")

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).

        Accepts filestrings of the form:
            chunk-<filename>.<ext>&<framenum>/<chunknum>
            file-<filename>
        """
        if DEBUGGING_MSG:
            # print file
        parsedform = parse_chunks(file)
        if parsedform:
            filename, framenum, chunks, user_or_cache = parsedform
            try:
                # filename should be prefixed by "file-" in order to be valid.
                # frame number is expected to exist for this cache.
                chunksdir = 'video-' + filename
                framedir = filename + '.' + framenum + '.dir'
                path = self.movies_path + '/' + chunksdir + '/' + framedir
                # get chunks list and open up all files
                files = self.get_chunk_files(path, chunks)
                # return CacheHandler.chunks[index]

                if DEBUGGING_MSG:
                    print "chunks requested:", chunks
                    print 'chunksdir', chunksdir
                    print 'framedir', framedir
                    print 'path', path
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)

            parentCache = self.parentCache
            # print '[cache.py] primal_x to this link was', parentCache.primal_x[self.index]
            packet_size = parentCache.movie_LUT.chunk_size_lookup(filename)
            rate_per_chunk = packet_size / 1000 / BUFFER_LENGTH * 8 # (Kbps)
            parentCache.primal_x[self.index] = rate_per_chunk * len(chunks)
            # print '[cache.py] primal_x is forced down to', parentCache.primal_x[self.index]

            #CacheHandler.connected[self.index] = True
            CacheHandler.watching_video[self.index] = filename
            producer = self.chunkproducer(files, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=None, cmd="RETR")
            return

    def on_connect():
        print '[cache.py] CONNECTION is ESTABLISHED!!'

    def get_chunk_files(self, path, chunks=None):
        """For the specified path, open up all files for reading. and return
        an array of file objects opened for read.

        Only return the file objects specified by chunk numbers argument."""
        files = Queue.Queue()
        if not chunks:
            return files
        file_list_iterator = self.run_as_current_user(self.fs.get_list_dir, path)
        file_list = list(file_list_iterator)

        # print "[cache.py]", chunks
        for x in range(len(file_list)):
            each_file = file_list[x]
            # print "[cache.py]", x, "th file", each_file
            filename = ((each_file.split(' ')[-1]).split('\r'))[0]
            chunk_num = (each_file.split('_')[0]).split('.')[-1]
            if chunk_num.isdigit() and int(chunk_num) in chunks:
                filepath = path + '/' + filename
                # print "filepath ", filepath
                fd = self.run_as_current_user(self.fs.open, filepath, 'rb')
                files.put(fd)
                if (DEBUGGING_MSG):
                    print "Sending chunk", filename

        return files

class ServerDownloader(threadclient.ThreadClient, threading.Thread):
    """
    Requests new chunks from the server. Is always connected to the server.

    Since the chunk size is always fixed, fix the expected packet_size.
    """
    def __init__(self, address, packet_size):
        threading.Thread.__init__(self)
        StreamFTP.__init__(self, address, chunk_size=packet_size)
        self.client.set_callback(self.chunkcallback)

    def put_instruction(self, cmd_string):
        """Something or other"""
        pass

    def chunkcallback(self, chunk_size, fname):
        # directory name by convention is filename itself.
        def helper(data):
            file_to_write = open(fname, 'a+b')
            datastring = data + chunk_num_and_data[1]
            curr_bytes = sys.getsizeof(datastring)
            outputStr = "%s: Received %d bytes. Current Total: %d bytes.\n" % \
                (filestr, sys.getsizeof(data), curr_bytes)
            sys.stdout.write(outputStr)
            sys.stdout.flush()
            if DEBUGGING_MSG:
                outputStr = "Writing %d bytes to %s.\n" %  (curr_bytes, filestr)
            sys.stdout.write(outputStr)
            sys.stdout.flush()
            file_to_write.write(datastring)
            file_to_write.close()
        return helper

def load_cache_config(cache_id):
    f = open(cache_config_file)
    fs = csv.reader(f, delimiter = ' ')
    for row in fs:
        if int(row[0]) == cache_id:
            if (DEBUGGING_MSG): print '[cache.py] Cache configuration : ', row
            return row
    # If not found
    return None

def get_server_address(tracker_address):
    return retrieve_server_address_from_tracker(tracker_address)

def main():
    if len(sys.argv) == 2:
        config = load_cache_config(int(sys.argv[1])) # Look up configuration of the given cache ID
        if config == None:
            print '[cache.py] cache_id not found'
            sys.exit()
    else:
        print '[cache.py] cache.py needs an argument "cache_id"'
        sys.exit()

    #resource.setrlimit(resource.RLIMIT_NOFILE, (5000,-1))
    cache = Cache(config)
    cache.start_cache()

if __name__ == "__main__":
    main()
