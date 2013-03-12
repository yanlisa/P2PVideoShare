from streamer import StreamFTP
from threadclient import *
from server import *
from pyftpdlib import ftpserver
import Queue
import random
import csv
import time
import threading
from helper import *

# Debugging MSG
DEBUGGING_MSG = True
# Cache Configuration
cache_config_file = '../../config/cache_config.csv'

MAX_CONNS = 10
MAX_VIDEOS = 10
BUFFER_LENGTH = 10
path = "."
tracker_address = "http://localhost:8080/req/"

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
        print '[cache.py] ', self.handlers
        print self.handlers
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
        #   masq_ip,
        #   stream_rate,
        #   num_of_chunks_cache_stores ]
        self.packet_size = 2504
        server_ip_address = get_server_address(tracker_address)
        self.server_client = ThreadClient(server_ip_address, self.packet_size)
        # Cache will get a response when each chunk is downloaded.
        self.server_client.set_respond_RETR(True)

        cache_id = int(cache_config[0])
        address = (cache_config[1], int(cache_config[2]))
        register_to_tracker_as_cache(tracker_address, address[0], address[1])
        print '[cache.py] Address : ', address
        masq_address = cache_config[3]
        stream_rate = int(cache_config[4])
        # num_of_chunks_cache_stores = int(cache_config[5])

        # Variables for algorithms
        self.delta_x = 1
        self.delta_f = 1
        self.delta_la = 1
        self.delta_mu = 1
        self.delta_k = 1

        self.bandwidth_cap = 5000 # (Kbps)
        self.storage_cap_in_MB = 500 # (MB)
        self.storage_cap = self.storage_cap_in_MB * 1000 * 1000 # (Bytes)

        self.dual_la = 0 # dual variable for ST
        self.dual_mu = 0 # dual variable for BW

        self.primal_x = [] * MAX_CONNS
        self.primal_f = [] * MAX_VIDEOS
        self.dual_k = [] * MAX_VIDEOS

        self.sum_rate = 0
        self.sum_storage = 0

        self.authorizer = ftpserver.DummyAuthorizer()
        # allow anonymous login.
        self.authorizer.add_anonymous(path, perm='elr')
        handler = CacheHandler
        # handler.timeout = 1
        handler.authorizer = self.authorizer
        self.movie_LUT = retrieve_MovieLUT_from_tracker(tracker_address)
        handler.movie_LUT = self.movie_LUT
        # handler.masquerade_address = '107.21.135.254'
        handler.passive_ports = range(60000, 65535)

        handler.rates = [0]*MAX_CONNS # in chunks per frame
        # handler.chunks = [chunks]*MAX_VIDEOS
        handler.chunks = {}
        handler.binary_g = [0]*MAX_CONNS # binary: provided chunks sufficient for conn
        handler.watching_video = ['']*MAX_CONNS

        self.mini_server = ThreadStreamFTPServer(address, handler, stream_rate)
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
        print '[[[cache.py]]]', new_chunks
        CacheHandler.chunks[video_name] = new_chunks
        # self.mini_server.get_handlers()[index].set_chunks(new_chunks)

    def get_g(self, index):
        return CacheHandler.binary_g[index]

    def get_watching_video(self, index):
        return CacheHandler.watching_video[index]

    def rate_update(self):
        print '[cache.py] rate updating'
        handlers = self.get_handlers()
        if len(handlers) == 0:
            print '[cache.py] No user is connected'
        else:
            sum_rate = 0
            for i in range(len(handlers)):
                handler = handlers[i]
                if handler._closed == True:
                    continue
                video_name = self.get_watching_video(i)
                packet_size = self.movie_LUT.chunk_size_lookup(video_name)
                current_rate = self.get_conn_rate(i)
                sum_rate = sum_rate + current_rate * packet_size / 1000 / BUFFER_LENGTH * 8
            self.sum_rate = sum_rate
            print '[cache.py] BW usage ' + str(self.sum_rate) + '/' + str(self.bandwidth_cap)

            for i in range(len(handlers)):
                handler = handlers[i]
                if handler._closed == True:
                    print '[cache.py] Connection ' + str(i) + ' is closed'
                    continue
                video_name = self.get_watching_video(i)
                print '[cache.py] User ' + str(i) + ' is watching ' + str(video_name)
                packet_size = self.movie_LUT.chunk_size_lookup(video_name)
                if packet_size == 0:
                    continue
                additional_rate_needed = packet_size / 1000 / BUFFER_LENGTH * 8 # (Kbps)

                current_rate = self.get_conn_rate(i)
                max_possible_rate = len(self.get_chunks(video_name)) # FIX : it should be video(i)

                g = self.get_g(i)
                print '[cache.py] User ' + str(i) + ' (g,cur_rate,max_rate) ' + str(g) + ',' + str(current_rate) + ',' + str(max_possible_rate)
                print '[cache.py] Cache (sum_rate, additional_rate, bw_cap) ', (self.sum_rate, additional_rate_needed, self.bandwidth_cap)
                if g == 1 and current_rate < max_possible_rate + 1 and self.sum_rate + additional_rate_needed < self.bandwidth_cap:
                    # Increase rate assigned to this link.
                    self.set_conn_rate(i, current_rate + 1)
                    self.sum_rate = self.sum_rate + additional_rate_needed
                    print '[cache.py] Rate updated'
                    print '[cache.py] BW Usage', self.sum_rate , '(Kbps) /' , self.bandwidth_cap , '(Kbps)'

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
        server_request_string = '%'.join(server_request)
        server_request_string = server_request_string + '&' + str(0)

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
            fname, framenum, binary_g, chunks = parsed_form
            print '[cache.py] Finished downloading: Frame %s Chunk %s' % (framenum, chunks)

            # while True:
            #     time.sleep(0.5)
            #     folder_name = 'video-' + video_name + '/' + video_name + '.' + str(i) + '.dir/'
            #     if chunk_exists_in_frame_dir(folder_name, index) == True:
            #         break
        return True

    def storage_update(self):
        print '[cache.py] storage updating'
        handlers = self.get_handlers()
        if len(handlers) == 0:
            print '[cache.py] No user is connected'
        else:
            for i in range(len(handlers)):
                handler = handlers[i]
                if handler._closed == True:
                    print '[cache.py] Connection ' + str(i) + ' is closed'
                    continue

                # Open connection
                current_rate = self.get_conn_rate(i)
                video_name = self.get_watching_video(i)
                packet_size = self.movie_LUT.chunk_size_lookup(video_name)
                frame_num = self.movie_LUT.frame_num_lookup(video_name)
                if packet_size == 0:
                    continue

                max_possible_rate = len(self.get_chunks(video_name))
                additional_storage_needed = packet_size * 20
                if current_rate > max_possible_rate and self.sum_storage + additional_storage_needed < self.storage_cap:
                    # Download one more chunk across all frames
                    chunk_index = random.sample(range(0, 40), 1)
                    if self.download_one_chunk_from_server(video_name, chunk_index) == True:
                        self.set_chunks(video_name, list(set(self.get_chunks(video_name)) | set(map(str, chunk_index))))
                        self.sum_storage = self.sum_storage + additional_storage_needed
                        print '[cache.py] storage update done'
                        print '[cache.py] storage Usage' , int(self.sum_storage/1000/1000) , '(MB) /' , int(self.storage_cap/1000/1000) , '(MB)'

    def topology_update(self):
        print '[cache.py] topology updating'

    def connection_check(self):
        print '[cache.py] connection checking'
        #conns = self.get_conns()

        # Currently assuming 'a single movie'. It needs to be generalized
        #for i in range(len(conns)):
        #CacheHandler.connected[self.index] = True

    def start_control(self):
        T_rate = 1
        T_storage = 10
        T_topology = 30

        T_gcd = 1
        T_lcm = 30

        T_ct = 0

        while True:
            print '[cache.py] T_ct = ', T_ct
            time.sleep(T_gcd)
            T_ct = T_ct + T_gcd
            if T_ct % T_rate == 0:
                self.rate_update()
            if T_ct % T_storage == 0:
                self.storage_update()
            if T_ct & T_topology == 0:
                self.topology_update()
                self.connection_check()

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
        print "Packet rate has been set within CacheHandler for: %d conn to rate %d" % (self.index, new_rate)

    def ftp_CNKS(self, arg):
        """
        FTP command: Returns this cache's chunk number set.
        """
        print "index:", self.index
        print '[cache.py] CacheHandler.chunks', CacheHandler.chunks
        print '[cache.py] line', arg
        video_name = arg.split('file-')[-1].split('.')[0]
        print '[cache.py] video_name ', video_name
        if video_name in CacheHandler.chunks.keys():
            data = '%'.join(map(str, CacheHandler.chunks[video_name]))
        else:
            data = '%'.join(map(str, ''))
        data = data + '&' + str(CacheHandler.rates[self.index])
        print "Sending CNKS: ", data
        #CacheHandler.connected[self.index] = True
        self.push_dtp_data(data, isproducer=False, cmd="CNKS")

    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).

        Accepts filestrings of the form:
            chunk-<filename>.<ext>&<framenum>/<chunknum>
            file-<filename>
        """
        if DEBUGGING_MSG:
            print file
        parsedform = parse_chunks(file)
        if parsedform:
            filename, framenum, binary_g, chunks = parsedform
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
                    print 'binary_g', binary_g
            except OSError, err:
                why = ftpserver._strerror(err)
                self.respond('550 %s.' % why)

            CacheHandler.binary_g[self.index] = binary_g
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
        iterator = self.run_as_current_user(self.fs.get_list_dir, path)

        print "[cache.py]", chunks
        for x in range(len(chunks)):
            liststr = iterator.next()
            filename = ((liststr.split(' ')[-1]).split('\r'))[0]
            chunk_num = (filename.split('_')[0]).split('.')[-1]
            if chunk_num.isdigit() and int(chunk_num) in chunks:
                filepath = path + '/' + filename
                print "filepath ", filepath
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

    cache = Cache(config)
    cache.start_cache()

if __name__ == "__main__":
    main()
