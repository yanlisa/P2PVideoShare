from streamer import StreamFTP
import os, sys, errno
import time

class ThreadClient(object):
    """Creates a client thread and pushes instructions to it.
    Also can close client socket arbitrarily.

    This class unfortunately crosses the data abstraction layer, but I was not
    sure of a better way to implement this.
    """
    def __init__(self, address, packet_size):
        """
        Makes a StreamFTP thread and starts it.
        """
        self.client = StreamFTP(address)
        self.client.set_chunk_size(packet_size)
        self.client.set_callback(self.chunkcallback)
        self.instr_queue = self.client.get_instr_queue()
        self.resp_queue = self.client.get_resp_queue()
        self.client.start()

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
                print "Socket closed. Errors:", sys.exc_info()[0]
                return True
        return False

    def get_response(self):
        try:
            response_string = self.resp_queue.get()
            return response_string
        except:
            return None

    def chunkcallback(self, chunk_size, fname):
        chunk_num_and_data = [0, '']
        print "Expected chunk_size:", chunk_size
        header_and_total_chunk = (37, chunk_size) # header is 37B
        expected_threshold = [header_and_total_chunk[1]]
    
        # directory name by convention is filename itself.
        os.mkdir(fname)
    
        def helper(data):
            filestr = fname + '/' + fname + '.' + str(chunk_num_and_data[0])
            datastring = data + chunk_num_and_data[1]
            curr_bytes = sys.getsizeof(datastring)
            outputStr = "%s: Received %d bytes. Current Total: %d bytes.\n" % \
                (filestr, sys.getsizeof(data), curr_bytes)
            sys.stdout.write(outputStr)
            sys.stdout.flush()
            # print "Current", str(curr_bytes), "vs. expected", str(expected_threshold[0])
            if curr_bytes >= expected_threshold[0]:
                outputStr = "Writing %d bytes to %s.\n" % \
                    (curr_bytes, filestr)
                sys.stdout.write(outputStr)
                sys.stdout.flush()
                file_to_write = open(filestr, 'wb')
                file_to_write.write(datastring)
                file_to_write.close()
                # reset
                chunk_num_and_data[1] = '' # new data string
                expected_threshold[0] = header_and_total_chunk[1] # new threshold.
                chunk_num_and_data[0] += 1 # new file extension
            else:
                chunk_num_and_data[1] = datastring
                # expecting one more packet, so add a header size.
                expected_threshold[0] += header_and_total_chunk[0]
    
        return helper

if __name__ == "__main__":
    packet_size = 2500
    fname = "file-Abracadabra.1"
    if len(sys.argv) != 3:
        print "Usage: python threadclient.py <chunk_size> <filename>"
    else:
        chunk_size = int(sys.argv[1])
        fname = sys.argv[2]
        thread_client = ThreadClient('107.21.135.254', chunk_size)
        thread_client.put_instruction('LIST')
        print thread_client.get_response()
        thread_client.put_instruction('RETR ' + fname)
        time.sleep(3)
        thread_client.kill_transfer()
        thread_client.put_instruction('QUIT')
