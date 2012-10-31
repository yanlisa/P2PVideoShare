from client import *

class P2PUser():

    def __init__(self):
        self.clients = []
        for i in xrange(4): # TODO: instantiate threaded-client wrappers instead
            self.clients[i] = StreamFTP('107.21.135.254') # TODO: ask tracker?

    def play(self, video_name, seek_time):
        """ Starts playing the video as identified by either name or number and
        begins handling the data connections necessary to play the video,
        starting at seek_time.
        """
        # start timer
        # ask all clients for all chunks in the next 10 second set
        # after 10 sec, cancel client connections
        pass
