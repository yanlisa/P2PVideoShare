import wx
import wx.lib.platebtn
import os

class MainFrame(wx.Frame):
    """The main window has to deal with events.
    """
    def __init__(self, title, controller=None):
        self.controller = controller
        wx.Frame.__init__(self, None, -1, title,
                          pos=wx.DefaultPosition, size=(800, 800))
        mainpanel = wx.Panel(self, -1)
        sizer_mainpanel = wx.BoxSizer(wx.VERTICAL)

        videos = controller.get_list_of_video()
        for each_video in videos:
            each_text = wx.lib.platebtn.PlateButton(mainpanel, label=each_video)
            self.Bind(wx.EVT_BUTTON, self.OnClick, each_text)
            sizer_mainpanel.Add(each_text, 1)

        mainpanel.SetSizer(sizer_mainpanel)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(mainpanel, 1, flag=wx.EXPAND)
        self.SetSizer(sizer)
        self.SetMinSize((350, 300))
        self.Centre()
        self.Bind(wx.EVT_CLOSE, self.OnExit)

    def OnExit(self, evt):
        self.controller.open_video_frame()
        self.Destroy()

    def OnClick(self, evt):
        video_name = evt.GetEventObject().GetLabelText()
        self.controller.watch_video(video_name)
