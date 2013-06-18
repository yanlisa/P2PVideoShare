from streamer import StreamFTP
import os, sys, errno
import time
from helper import parse_chunks

DEBUGGING_MSG = False

class ThreadClient(object):
    """Creates a client thread and pushes instructions to it.
    Also can close client socket arbitrarily.

    This class unfortunately crosses the data abstraction layer, but I was not
    sure of a better way to implement this.
    """
    def __init__(self, user, address, packet_size=0, client_id = 0):
        """
        Makes a StreamFTP thread and starts it.
        """
        self.user = user
        self.address = address
        self.client = StreamFTP(user, address)
        self.client.set_chunk_size(packet_size)
        self.client.set_callback(self.chunkcallback)
        self.instr_queue = self.client.get_instr_queue()
        self.resp_queue = self.client.get_resp_queue()
        self.client.start()
        self.chunks = None
        self.client_id = client_id

    def set_respond_RETR(self, flag):
        self.client.resp_RETR = flag

    def put_instruction(self, cmd_string):
        """
        Puts an FTP command into the client's instruction queue. The client
        will call the instruction once it finishes its current instruction.
        """
        self.instr_queue.put(cmd_string)

    def kill_transfer(self):
        """
        If a transfer connection exists, kill it and catch errors.
        """
        if self.client.conn is not None:
            try:
                self.client.conn.close()
            except:
                if DEBUGGING_MSG:
                    print "Socket closed. Errors:", sys.exc_info()[0]
                return True
        return False

    def get_response(self):
        """
        Receive response string from the shared response queue.
        """
        print '[threadclient.py] get_response() is called'
        try:
            response_string = self.resp_queue.get()
            return response_string
        except:
            return None

    def set_chunks(self, chunks):
        """
        Set the expected chunks from the cache.
        This chunk list is used to save the file names.
        """
        start = chunks.find('[')
        end = chunks.find(']')
        chunkstr = chunks[start+1:end]
        self.chunks = sorted(map(int, (chunkstr.split(', '))))
        self.client.set_chunks(self.chunks)

    def chunkcallback(self, chunk_size, fnamestr):
        order_and_data = [0, '']
        expected_threshold = [chunk_size]

        parsed_form = parse_chunks(fnamestr)
        if parsed_form:
            fname, framenum, chunks, user_or_cache = parsed_form
            video_name = fname
            fname = fname + '.' + framenum

        video_dirname = 'video-' + video_name
        dirname = video_dirname + '/' + fname + '.dir'
        try:
            os.mkdir(video_dirname)
        except:
            pass

        try:
            os.mkdir(dirname)
        except:
            pass

        def helper(data):
            if order_and_data[0] >= len(chunks):
                pass
            else:
                filestr = dirname + '/' + fname + '.' + str(chunks[order_and_data[0]]).zfill(2) + '_40.chunk'
                total_curr_bytes = len(order_and_data[1]) + len(data)
                extra_bytes = total_curr_bytes - expected_threshold[0]
                if extra_bytes < 0: # expecting more tcp packets
                    order_and_data[1] = ''.join([order_and_data[1], data])
                else:
                    trunc_data = data
                    if extra_bytes > 0:
                        trunc_data = data[:len(data)-extra_bytes]
                    datastring = ''.join([order_and_data[1], trunc_data])
                    if DEBUGGING_MSG:
                        outputStr = "Writing %s (actual: %d, expected: %d, totalCurrBytes: %d).\n" % \
                            (filestr, len(datastring), chunk_size, total_curr_bytes)
                        sys.stdout.write(outputStr)
                        sys.stdout.flush()
                    file_to_write = open(filestr, 'wb')
                    file_to_write.write(datastring)
                    file_to_write.close()
                    # reset
                    order_and_data[1] = '' # new data string
                    order_and_data[0] += 1 # new file extension

                    if extra_bytes > 0:
                        order_and_data[1] = data[len(data)-extra_bytes:]

        if len(chunks) == 0:
            # If empty chunks, just do nothing with received data from RETR.
            return lambda data : None
        else:
            # Save received chunks to files inside directory.
            return helper
