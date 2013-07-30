import wx # 2.8
import vlc
import os
import time
import string
import random
import user2

from gui.mainframe import MainFrame
from gui.videoframe import VideoFrame
from gui.infoframe import InfoFrame

class UserClient():
# Controller
    def __init__(self):
        self.app = wx.PySimpleApp()

    def run(self):
        self.open_logo_frame()
        self.app.MainLoop()

    def open_logo_frame(self):
        self.main_frame = MainFrame("MASCOTS 2013 Demo", self)
        self.main_frame.Centre()
        self.main_frame.Show(True)

    def open_video_frame(self, video_path):
        self.video_frame = VideoFrame("MASCOTS 2013 Demo", self, video_path)
        self.video_frame.Centre()
        self.video_frame.Show(True)

    def open_info_frame(self):
        self.info_frame = InfoFrame("MASCOTS 2013 Demo", self)
        self.info_frame.SetPosition((500,200))
        self.info_frame.Show(True)

    def close_video_frame(self):
        self.user.disconnect(self.tracker_address, self.video_name, self.user_name)
        self.video_frame.Destroy()
        self.timer2.Stop()

    def get_list_of_video(self):
        return user2.movies

    def watch_video(self, video_name):
        self.video_name = video_name
        self.tracker_address = user2.tracker_address
        self.user_name = user_name = 'user-' + ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(6))
        self.user = user = user2.P2PUser(user2.tracker_address, video_name, user_name, self)
        user.start()

        print user.frame_number
        self.video_path = video_path = os.path.join(
                    os.getcwd(),
                    'video-' + video_name + '/' + video_name + '.flv'
                    )
        self.open_video_frame(video_path)
        #self.open_info_frame()

        self.timer = wx.Timer(self.main_frame)
        self.main_frame.Bind(wx.EVT_TIMER, self.on_timer, self.timer)
        self.timer.Start(100) # start timer after a delay

    def on_timer(self, evt):
        if self.user.frame_number > 2:
            self.video_frame.playVideo(self.video_path)
            self.timer.Stop()
            self.timer2 = wx.Timer(self.video_frame)
            self.video_frame.Bind(wx.EVT_TIMER, self.on_timer2, self.timer2)
            self.timer2.Start(1000) # start timer after a delay

    def on_timer2(self, evt):
        self.video_frame.infoText.SetLabel("%.1f%% of the stream is from caches." % self.user.ratio_from_cache)
        self.video_frame.Update()

if __name__ == "__main__":
    user_client = UserClient()
    user_client.run()
