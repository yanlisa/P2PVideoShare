import os
import urllib2
import csv
import commands
import Queue

DEBUGGING_MSG = True

(temp, temp_str1) = commands.getstatusoutput('~/ftp-git/tracker_ip.sh')
temp_str2 = temp_str1.replace('.','-').split('-')
tracker_address = 'http://' + '.'.join(temp_str2[1:5]) + ':8081/req/'
#tracker_address = 'http://0.0.0.0:8081/req/'
print temp_str2
print tracker_address

def load_tracker_address():
    return tracker_address

class MovieLUT():
    """
    Lookups for data that will eventually be moved to Tracker. For now, load a config
    file and always have that config file set.
    """
    def __init__(self):
        self.frame_num_index = 0
        self.code_param_n_index = 1
        self.code_param_k_index = 2
        self.size_bytes_index = 3
        self.chunk_size_index = 4
        self.last_chunk_size_index = 5
        self.movies_LUT = {}

    def update_with_csv(self, config_file):
        f = open(config_file)
        fs = csv.reader(f, delimiter = ' ')
        for row in fs:
            if (DEBUGGING_MSG): print '[server.py] Loading movie : ', row
            movie_name = row[0]
            self.movies_LUT[movie_name] = (int(row[1]), int(row[2]), int(row[3]), int(row[4]), int(row[5]), int(row[6]))

    def gen_lookup(self, video_name, feature_index):
        """Assumes all features are integers, so return 0 if the video doesn't exist."""
        if video_name in self.movies_LUT:
            return self.movies_LUT[video_name][feature_index]
        else:
            # print '[helper.py] The video ', video_name, ' does not exist.'
            # print '[helper.py] Feature index =', feature_index
            return 0

    def frame_num_lookup(self, video_name):
        """Number of frames for this video."""
        return self.gen_lookup(video_name, self.frame_num_index)

    def size_bytes_lookup(self, video_name):
        """Size of total video, in bytes."""
        return self.gen_lookup(video_name, self.size_bytes_index)

    def chunk_size_lookup(self, video_name):
        """Size of chunk, minus last chunk, in bytes."""
        return self.gen_lookup(video_name, self.chunk_size_index)

    def last_chunk_size_lookup(self, video_name):
        """Size of last chunk, in bytes."""
        return self.gen_lookup(video_name, self.last_chunk_size_index)

    def code_param_n_lookup(self, video_name):
        """Code parameter n for this video."""
        return self.gen_lookup(video_name, self.code_param_n_index)

    def code_param_k_lookup(self, video_name):
        """Code parameter k for this video."""
        return self.gen_lookup(video_name, self.code_param_k_index)

def retrieve_caches_address_from_tracker(tracker_address, num_of_caches, user_name):
    req_str = 'GET_CACHES_ADDRESS&' + str(user_name) + '_' + str(num_of_caches)
    print '[helper.py] req_str :' + req_str
    ret_str = urllib2.urlopen(tracker_address + req_str).read()

    res = [''] * num_of_caches
    ret_str_split = ret_str.split('\n')
    ct = 0
    for each_row in ret_str_split:
        if each_row == '':
            return res[:ct]
        parsed_row = each_row.split(' ')
        if DEBUGGING_MSG:
            print '[helper.py] ', parsed_row
        res[ct] = [parsed_row[0], int(parsed_row[1])]
        ct = ct + 1
    return None

def retrieve_server_address_from_tracker(tracker_address):
    req_str = 'GET_SERVER_ADDRESS'
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    ret_str_split = ret_str.split(' ')
    return (ret_str_split[0], int(ret_str_split[1]))

def retrieve_server_address_from_tracker_for_cache(tracker_address):
    req_str = 'GET_SERVER_ADDRESS_FOR_CACHE'
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    ret_str_split = ret_str.split(' ')
    return (ret_str_split[0], int(ret_str_split[1]))

def retrieve_MovieLUT_from_tracker(tracker_address): # Retrieve it from the tracker
    lut = MovieLUT()

    req_str = 'GET_ALL_VIDEOS'
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    ret_str_split = ret_str.split('\n')
    for each_row in ret_str_split:
        if each_row == '':
            break
        parsed_row = each_row.split(' ')
        print parsed_row
        lut.movies_LUT[parsed_row[1]] = (int(parsed_row[2]), int(parsed_row[3]), int(parsed_row[4]), int(parsed_row[5]), int(parsed_row[6]), int(parsed_row[7]))
    return lut

def register_to_tracker_as_cache(tracker_address, ip, port):
    req_str = 'REGISTER_CACHE&' + str(ip) + '_' + str(port)
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    return ret_str

def register_to_tracker_as_user(tracker_address, ip, port, watching_movie):
    req_str = 'REGISTER_USER&' + str(ip) + '_' + str(port) + '_' + str(watching_movie)
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    return ret_str

def deregister_to_tracker_as_user(tracker_address, ip, port, watching_movie):
    req_str = 'REMOVE_USER&' + str(ip) + '_' + str(port) + '_' + str(watching_movie)
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    return ret_str

def update_chunks_for_cache(tracker_address, ip, port, video_name, chunks):
    req_str = 'UPDATE_CHUNKS_FOR_CACHE&' + str(ip) + '_' + str(port) + '_' + str(video_name) + '_' + str(chunks)
    req_str = req_str.replace(" ", "") # Having space in a query makes a problem
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    return ret_str

def update_server_load(tracker_address, video_name, num_of_chks_from_server):
    req_str = 'UPDATE_SERVER_LOAD&' + str(video_name) + '_' + str(num_of_chks_from_server)
    req_str = req_str.replace(" ", "") # Having space in a query makes a problem
    print '[helper.py] query_str ', req_str
    ret_str = urllib2.urlopen(tracker_address + req_str).read()
    print '[helper.py] ret_str ', ret_str
    return ret_str

def chunk_delete_all_in_frame_dir(folder_name):
    try:
        filelist = os.listdir(folder_name)
        for f in filelist:
            file_path = os.path.join(folder_name, f)
            print '[helper.py] deleting file', file_path
            if os.path.isfile(file_path):
                os.unlink(file_path)
    except:
        pass

def chunk_exists_in_frame_dir(folder_name, chunk_index):
    # returns True if the chunk exists
    chunksNums = []
    try:
        for chunk_name in os.listdir(folder_name):
            chunk_suffix = (chunk_name.split('_')[0].split('.'))[-1]
            if chunk_suffix.isdigit():
                if int(chunk_suffix) == int(chunk_index[0]):
                    return True
        return False
    except:
        return False

def chunk_nums_in_frame_dir(folder_name):
    # returns an array of chunk numbers (ints) in this frame.
    # folder_name ends in '/'.
    # assumes chunk filenames end in chunk number.
    try:
        chunksNums = []
        for chunk_name in os.listdir(folder_name):
            chunk_suffix = (chunk_name.split('_')[0].split('.'))[-1]
            if chunk_suffix.isdigit():
                chunksNums.append(chunk_suffix)
        return chunksNums
    except:
        return []

def chunk_files_in_frame_dir(folder_name):
    # opens file objects for each file in the directory.
    # folder_name ends in '/'.
    try:
        chunksList = []
        for chunk_name in os.listdir(folder_name):
            chunkFile = open(folder_name + chunk_name, 'rb')
            chunksList.append(chunkFile)
        return chunksList
    except:
        return []

def parse_chunks(arg):
    """Returns file name, chunks, and frame number.
    File string format:
        file-<filename>.<framenum>.<chunk1>%<chunk2>%<chunk3>&user_or_cache

    """
    filestr = arg.split('&')[0]
    binarystr = arg.split('&')[1]
    if filestr.find('file-') != -1:
        filestr = (filestr.split('file-'))[-1]

    parts = filestr.split('.')
    if len(parts) < 2:
            return None
    filename, framenum = parts[0], parts[1]
    if len(parts[2]) == 0:
        return (filename, framenum, [], int(binarystr))
    else:
        chunks = map(int, (parts[2]).split('%'))
        return (filename, framenum, chunks, int(binarystr))

def cache_chunks_to_request(available_chunks, rates, code_param_n, code_param_k):
    """
    (a) Sort the packets by their rarity (defined by the presence of packets in multiple caches).
    (b) Starting with the rarest first, assign the rarest packet to the cache with the lowest used BW ratio currently. Pop off the rarest packet and decrement the bandwidth of the assigned cache.
    (c) Repeat until all packets have been assigned a cache source, or until 20 chunks have been assigned.
    """

    # index assignment here
    chunk_locs = {}
    assigned_chunks = [list()]*len(rates)
    for i in range(len(rates)):
        for j in available_chunks[i]:
            if rates[i]:
                if j in chunk_locs:
                    (chunk_locs[j]).append(i)
                else:
                    chunk_locs[j] = [i]

    # sort chunks by rarest first
    chunk_freqs = Queue.PriorityQueue()
    for chunk in chunk_locs:
        chunk_freqs.put((len(chunk_locs[chunk]), chunk))

    # from rarest first, make chunk request list by assigning next available chunk
    # to carrier with the lowest ratio of used cache rate so far (fairness)
    for i in range(code_param_k):
        if chunk_freqs.empty():
            break
        freq, chunk = chunk_freqs.get()
        best_location = -1
        for cache in chunk_locs[chunk]:
            ratio_bw_used = float(len(assigned_chunks[cache]))/rates[cache]
            if best_location == -1:
                if ratio_bw_used < 1:
                    best_location = cache
                    # print "No best_location set for chunk %s, as ratio bw is %d: %f" % (chunk, cache, ratio_bw_used)
            else:
                best_locations_ratio_bw = float(len(assigned_chunks[best_location]))/rates[best_location]
                # print "%d:%f vs. %d:current best %f" % (cache, ratio_bw_used, best_location, best_locations_ratio_bw)
                if ratio_bw_used < best_locations_ratio_bw:
                    best_location = cache
                    # print "best location for chunk %s determined to be %d" % (chunk, best_location)
        if best_location == -1:
            continue
        if not assigned_chunks[best_location]:
            assigned_chunks[best_location] = [str(chunk)]
        else:
            (assigned_chunks[best_location]).append(str(chunk))

    return assigned_chunks
