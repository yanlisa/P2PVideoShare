from streamer import StreamFTP
import os, sys, errno
import time

# parse filename to extract filename, ext, framenum, chunks.  
def parse_chunks(filestr):
    """Returns file name, extension, chunks, and frame number.
    File string format:
        file-<filename>.<ext>.<framenum>.<chunk1>%<chunk2>%<chunk3>
     """
    if filestr.find('file-') != -1:
        filestr = (filestr.split('file-'))[-1]
        print filestr
        parts = filestr.split('.')
        if len(parts) < 3:
            return None
        filename, ext, framenum = parts[0], parts[1], parts[2]
        if len(parts) == 4:
            chunks = map(int, (parts[3]).split('%'))
        else:
            chunks = None
        return (filename, ext, framenum, chunks)
    
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
        self.chunks = None

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
        """
            Receive response string from the shared response queue.
        """
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
        print "Expected chunk_size:", chunk_size
        header_and_total_chunk = (37, chunk_size) # header is 37B
        expected_threshold = [header_and_total_chunk[1]]

        parsed_form = parse_chunks(fnamestr)
        chunks = None
        if parsed_form:
            fname, ext, framenum, chunks = parsed_form
            fname = fname + '.' + framenum
        else:
            fname = fnamestr

        if not chunks:
            chunks = self.chunks

        # directory name by convention is filename itself.
        if not os.path.isdir(fname):
            os.mkdir(fname)
    
        def helper(data):
            filestr = fname + '/' + fname + '.' + str(chunks[order_and_data[0]])
            datastring = data + order_and_data[1]
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
                order_and_data[1] = '' # new data string
                expected_threshold[0] = header_and_total_chunk[1] # new threshold.
                order_and_data[0] += 1 # new file extension
            else:
                order_and_data[1] = datastring
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
        # thread_client = ThreadClient('107.21.135.254', chunk_size) #ec2
        # thread_client = ThreadClient('192.168.0.120', chunk_size) # home
        thread_client = ThreadClient('10.10.64.49', chunk_size) # airbears
        thread_client.put_instruction('LIST')
        print thread_client.get_response()
        thread_client.put_instruction('CNKS')
        chunks = thread_client.get_response()
        print chunks
        thread_client.set_chunks(chunks)
        fname_without_chunks = 'file-' + (fname.split('-')[-1]).split('.')[0]
        thread_client.put_instruction('VLEN ' + fname_without_chunks)
        print 'Number of frames:', thread_client.get_response()
        thread_client.put_instruction('RETR ' + fname)
        time.sleep(3)
        thread_client.kill_transfer()
        thread_client.put_instruction('QUIT')
