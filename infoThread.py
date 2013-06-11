import threading
from helper import *

# Update Information Period
T_update_info = 0.1
# Global parameters
CACHE_DOWNLOAD_DURATION = 8 # sec
SERVER_DOWNLOAD_DURATION = 2 # sec

class infoThread (threading.Thread):
    def __init__(self, video_name, code_param_n, code_param_k):
        threading.Thread.__init__(self)
        self.video_name = video_name
        self.code_param_n = code_param_n
        self.code_param_k = code_param_k

    def run(self):
        print 'Starting Info Exchange Thread'
        self.update_info()

    def update_info(self):
        filename = 'file-' + self.video_name + '.' + str(1)
        folder_name = 'video-' + self.video_name + '/' + self.video_name + '.' + str(1) + '.dir/'
        inst_CNKS = 'CNKS ' + self.filename
        inst_UPDG = 'UPDG '

        # self.clients : list of connected guys

        T_buffer = CACHE_DOWNLOAD_DURATION + SERVER_DOWNLOAD_DURATION
        ct_period = int(T_buffer / T_update_info)
        ct_loop = ct_period - 1

        clients_copy = []
        while True:
            ct_loop += 1
            if ct_loop == ct_period:
                # Copy self.clients to client_copy

                for each in clients_copy:
                    each_client = clients_copy.pop()
                    each_client.put_instruction('QUIT')

                for each in self.clients:
                    each_ip = each.address
                    each_client = ThreadClient(each_ip, self.packet_size, 1)
                    clients_copy.append(each_client)
                    each_client.put_instruction('ID %s' % self.user_name)
                ct_loop = 0

            available_chunks = [0]*len(clients_copy) # available_chunks[i] = cache i's availble chunks
            rates = [0]*len(clients_copy) # rates[i] = cache i's offered rate
            union_chunks = [] # union of all available indices
            for i in range(len(clients_copy)):
                client = clients_copy[i]
                client.put_instruction(inst_CNKS)
                return_str = client.get_response().split('&')
                if return_str[0] == '':
                    available_chunks[i] = []
                else:
                    available_chunks[i] = map(str, return_str[0].split('%'))
                    for j in range(len(available_chunks[i])):
                        available_chunks[i][j] = available_chunks[i][j].zfill(2)
                rates[i] = int(return_str[1])
                union_chunks = list( set(union_chunks) | set(available_chunks[i]) )

            ## index assignment here
            # Assign chunks to cache using cache_chunks_to_request.
            print '[user.py] Update_info_loop : Rates ', rates
            print '[user.py] Update_info_loop : Available chunks', available_chunks

            assigned_chunks = cache_chunks_to_request(available_chunks, rates, code_param_n, code_param_k)

            effective_rates = [0]*len(rates)
            for i in range(len(rates)):
                effective_rates[i] = len(assigned_chunks[i])

            chosen_chunks = [j for i in assigned_chunks for j in i]

            flag_deficit = int(sum(effective_rates) < code_param_k) # True if user needs more rate from caches

            # request assigned chunks
            for i in range(len(clients_copy)):
                client = clients_copy[i]
                print "[user.py] Update_info_loop : [Client " + str(i) + "] flag_deficit: ", flag_deficit, \
                    ", Assigned chunks: ", assigned_chunks[i]
                client.put_instruction(inst_UPDG + str(flag_deficit))

            sleep(T_update_info)

