import Queue
import random
import socket
import threading
import SocketServer

class TrackerServer(SocketServer.TCPServer):
    """Handles all connections to the tracker.
    """
    def __init__(self, address, handle):
        SocketServer.TCPServer.__init__(self, address, handle)

    # def run(self):
    #     self.serve_forever()

class TrackerHandler(SocketServer.BaseRequestHandler):
    """
    One handle is created per connection ot the server.
    """
    connection_lock = threading.Lock()
    connections = []

    def handle(self):
        # self.request is TCP socket connected to client
        while 1:
            address, port = self.request.getsockname()
            try:
                self.data = self.request.recv()
            except:
                print "Socket request to (%s, %s) closed." % str(address), str(port)
                break
            parsed_data = self.data.strip(' ')
            request_type = parsed_data[0]
            if request_type == 'a':
                # cache would like to be added to network.
                num_connections = 1
                if len(parsed_data) > 1:
                    num_connections = int(parsed_data[1])
                connections_lock.acquire()
                for i in range(num_connections):
                    connections.append(address)
                connections_lock.release()
            if request_type == 'd':
                # client terminated conn; cache has one more open connection port
                if len(parsed_data) > 1:
                    cache_address = parsed_data[1]
                    connections_lock.acquire()
                    connections.append(cache_address)
                    connections_lock.release()
                    if len(parsed_data) >= 3:
                        parsed_data = parsed_data[2:]
                        request_type = parsed_data[0]
            if request_type == 'r':
                # client requests connection to cache. Randomize and return.
                while len(connections) == 0:
                    pass
                connection_lock.acquire()
                index = random.randint(0, length(connections)-1)
                cache_address = connections.pop(index)
                connection_lock.release()

                send_str = 'c %s' % cache_address
                self.request.send(send_str)

class TrackerClientThread():
    """
    Connection to the tracker, open indefinitely.
    
    All send commands are managed by the TrackerClientThreadManager.
    """
    pass

class TrackerClientThreadManager()
    pass
if __name__ == "__main__":
    address = ("10.29.147.60", 59999)
    tracker = TrackerServer(address, TrackerHandler)
    # tracker.start()
    tracker.serve_forever()

