import wx
import os

class MainFrame(wx.Frame):
    """The main window has to deal with events.
    """
    def __init__(self, title):
        wx.Frame.__init__(self, None, -1, title,
                          pos=wx.DefaultPosition, size=(800, 800))

        # Menu Bar
        #   File Menu
        self.mainpanel = wx.Panel(self, -1)
        self.mainpanel.SetBackgroundColour(wx.WHITE)

        # Put everything togheter
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.mainpanel, 1, flag=wx.EXPAND)
        self.SetSizer(sizer)
        self.SetMinSize((350, 300))
        print 'hi'
        self.Centre()

    def OnExit(self, evt):
        """Closes the window.
        """
        self.Close()

