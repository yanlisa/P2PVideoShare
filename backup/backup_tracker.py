import Queue
import random
import socket
import threading
import SocketServer

class TrackerServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    """Handles all connections to the tracker.
    """
    pass

class TrackerHandler(SocketServer.BaseRequestHandler):
    """
    A 2D array (all_connections) of caches and available connections are
    stored at the class level; each element is a 2-element array [(address,
    dof), available_degrees], where dof is the available degrees of freedom.
    available_degrees is a mutable numthat will be in the range (0, dof).

    num_available is the number of caches whose available_degrees is non-zero.

    One handle is created per connection, and the handle function is called
    with an infinite while loop.
    """
    connections_lock = threading.Lock()
    connections = []
    cache_dict = {}

    def report_command(self, address, port, line):
        print "Received from (%s, %s): %s" % (address, port, line)

    def handle(self):
        # self.request is TCP socket. 
        address, portnum = self.request.getpeername()
        port = str(portnum)
        print "Connected to (%s, %s)." % (address, port)
        while 1:
            data = self.request.recv(1024)
            self.report_command(address, port, data)
            if not data:
                print "Socket closed to (%s, %s)." % (address, port)
                break
            parsed_data = data.split(' ')
            request_type = parsed_data[0]

            if True:
                print parsed_data

            if len(parsed_data) < 2:
                continue
            else:
                is_cache = parsed_data[1]

            if request_type == 'ADD' and is_cache == "1":
                # cache would like to be added to network.
                num_connections = 1
                if len(parsed_data) > 2:
                    num_connections = int(parsed_data[2])
                TrackerHandler.connections_lock.acquire()
                for i in range(num_connections):
                    TrackerHandler.connections.append((address, port))
                TrackerHandler.cache_dict[(address, port)] = num_connections
                TrackerHandler.connections_lock.release()

                if True:
                    print "Added cache to available connections. (%s, %s)." % (address, port)
                    print TrackerHandler.connections
            if request_type == 'DEL' and is_cache == "1":
                # cache wants to be removed from network.
                TrackerHandler.connections_lock.acquire()
                for i in range(len(TrackerHandler.connections)):
                    if TrackerHandler.connections(i) == (address, port):
                        TrackerHandler.connections.pop(i)
                TrackerHandler.cache_dict[(address, port)] = 0
                TrackerHandler.connections_lock.release()
                if True:
                    print "Removed cache from available connections. (%s, %s)." % (address, port)
            if request_type == 'CONN' and is_cache == "0":
                # client requests connection to cache. Randomize and return.
                if True:
                    print "Current available connections:", TrackerHandler.connections
                TrackerHandler.connections_lock.acquire()
                if len(TrackerHandler.connections) == 0:
                    line = "FAIL -1"
                    self.request.sendall(line)
                    TrackerHandler.connections_lock.release()
                    continue
                if len(parsed_data) == 4:
                    (prev_address, prev_port) = parsed_data[2], parsed_data[3]
                    if (prev_address, prev_port) in TrackerHandler.cache_dict:
                        # upper limit.
                        target_index = random.randint(0, len(TrackerHandler.connections) - \
                            TrackerHandler.cache_dict[(prev_address, prev_port)])
                        index = 0
                        for i in range(0, target_index):
                            if TrackerHandler.connections[i] == (prev_address, prev_port):
                                index += 1
                        if index < len(TrackerHandler.connections):
                            cache_address, cache_port = TrackerHandler.connections.pop(index)
                            TrackerHandler.cache_dict[(cache_address, cache_port)] -= 1
                        else:
                            # No new cache available.
                            line = "FAIL -1 NONEW%d" % index
                            self.request.sendall(line)
                            TrackerHandler.connections_lock.release()
                            continue
                        TrackerHandler.cache_dict[(prev_address, prev_port)] += 1
                else:
                    index = random.randint(0, len(TrackerHandler.connections)-1)
                    cache_address, cache_port = TrackerHandler.connections.pop(index)
                    TrackerHandler.cache_dict[(cache_address, cache_port)] -= 1
                TrackerHandler.connections_lock.release()
                if True:
                    print "Sending IP to (%s, %s): (%s, %s)" % (address, port, cache_address, cache_port)
                line = "REPL -1 %s %s" % (cache_address, cache_port)
                self.request.sendall(line)

class TrackerThread(threading.Thread, object):
    """
    Connection to the tracker.
    
    This default class is for a cache. The inherited class is for a client.
    """
    def __init__(self, server_addr, is_cache=0):
        """
        The default class is for a cache.
        """
        self.is_cache = is_cache
        self.sock = None
        self.server_addr = server_addr
        self.timeout = 30
        self.conn_to_server()
        print "Connected to server " + str(server_addr) + "."
        threading.Thread.__init__(self)

    def conn_to_server(self):
        self.sock = socket.create_connection(self.server_addr, self.timeout)

    def end_conn_to_server(self):
        self.sock.close()

    def make_command(self, cmd, opt_args=[]):
        line = cmd + " " + str(self.is_cache)
        for arg in opt_args:
            line += " " + str(arg)
        if True:
            print "Sending: " + line
        return line

class TrackerThreadCache(TrackerThread):
    def __init__(self, server_addr):
        TrackerThread.__init__(self, server_addr, is_cache=1)

    def add_self(self, num_conn=1):
        """
        Adds self as an available cache to the server. Optionally specify
        number of connections open.
        """
        if self.sock:
            line = self.make_command("ADD", [str(num_conn)])
            self.sock.sendall(line)
        else:
            "No connection to server."

    def close_self(self):
        """
        Removes self from server's system and close socket.
        """
        if self.sock:
            line = self.make_command("DEL")
            self.sock.sendall(line)
            self.end_conn_to_server()
        else:
            "No connection to server."

class TrackerThreadClient(TrackerThread):
    """
    Connection to the tracker.

    This class is for a client.
    """
    def __init__(self, server_addr):
        TrackerThread.__init__(self, server_addr, is_cache=0)

    def get_new_cache(self, prev_address=None, prev_port=None):
        if self.sock:
            opt_args = [prev_address, prev_port] if prev_address and prev_port else [] 
            line = self.make_command("CONN", opt_args)
            self.sock.sendall(line)
            data = self.sock.recv(1024)
            if True:
                print "Received: %s" % data
            parsed_data = data.split(' ')
            request_type = parsed_data[0]
            if request_type == 'REPL' and len(parsed_data) == 4:
                cache_address, cache_port = parsed_data[2], parsed_data[3]
                return (cache_address, cache_port)
        else:
            print "No connection to server."

if __name__ == "__main__":
    address = ("10.29.147.60", 59999)
    tracker = TrackerServer(address, TrackerHandler)
    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=tracker.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    while 1:
            pass
