FLV_VERBOSE = False
import struct
import thread
from datetime import datetime
import os
import sys

class FLVWrapper(object):
    # Tag types
    AUDIO = 8
    VIDEO = 9
    META = 18
    UNDEFINED = 0

    def __init__(self,writeFilePath, mode = 'w+b'):
        self.fname = writeFilePath
        self.mode = mode;
        self.tsp = self.tsr = 0; self.tsr0 = None;
        self.curPos = 0; self.curDecodableHeadPos = 0
        self.__DataSize = 0
        self.metaData = None                                      # meta data of video (dict)
        self.__lock = thread.allocate_lock()
        self.fp = open(writeFilePath, self.mode)
        self.isopen = True;

        if mode=='r+b':
            # read the header
            magic, version, flags, offset = struct.unpack('!3sBBI', self.fp.read(9))
            if FLV_VERBOSE: print 'FLV.open() hdr=', magic, version, flags, offset
            if magic != 'FLV': raise ValueError('This is not a FLV file')
            if version != 1: raise ValueError('Unsupported FLV file version')
            if offset > 9:
                self.fp.seek(offset-9, os.SEEK_CUR);
            self.fp.read(4);
            tagType = self.readbyte()                          # beginning of the first tag
            self.__DataSize = self.read24bit()                 # length of the message
            timeStamp = self.read24bit()                       # should be 0
            unknown = self.readint();                          # TimeStampExtended (8) + StreamID (24)
            if tagType == self.META:
                startpos = self.fp.tell();
                event = self.readAMFData(); self.metaData = self.readAMFData();
                endpos = self.fp.tell();
                self.fp.seek(self.__DataSize - (endpos-startpos),os.SEEK_CUR);   # this is a hack
            else:
                self.fp.read(self.__DataSize);
            self.close();
            self.isopen = False;

    def write(self,content):
        self.__lock.acquire();
        results = self.fp.write(content)
        self.curPos += len(content)
        self.__lock.release();
        return results

    # read until a certain storage amount from start to end (has to be on integer time lines)
    def readStorageAmount(self,startPos,endTime,writepath):
        self.curDecodableHeadPos = startPos                           # representing the start position
        tmpFile = open(writepath,'w+b')                               # write a temporary file
        # read the header
        if not self.isopen:
            self.fp = open(self.fname, self.mode)
            self.isopen = True
            self.curPos = 0; self.curDecodableHeadPos = 0
            self.tsp = self.tsr = 0; self.tsr0 = None;
            self.__DataSize = 0
            self.metaData = None                                      # meta data of video (dict)
        self.fp.seek(self.curDecodableHeadPos,os.SEEK_SET)            # jump to the start position

        if self.curDecodableHeadPos == 0:
            tmpData = self.fp.read(9)
            tmpFile.write(tmpData)
            magic, version, flags, offset = struct.unpack('!3sBBI', tmpData)
            self.curDecodableHeadPos += 9;
            if FLV_VERBOSE: print 'FLV.open() hdr=', magic, version, flags, offset
            if magic != 'FLV': raise ValueError('This is not a FLV file')
            if version != 1: raise ValueError('Unsupported FLV file version')
            if offset > 9:
                tmpFile.write(self.fp.read(offset-9));         # self.fp.seek(offset-9, os.SEEK_CUR);
                self.curDecodableHeadPos += offset - 9;
            tmpFile.write(self.fp.read(4)); self.curDecodableHeadPos += 4;
            # read the meta data (first read 11 bytes as usual, and then read DataSize)
            tagType = self.readbyte(); tmpFile.write(tagType)                  # beginning of the first tag
            self.__DataSize = self.read24bit(); tmpFile.write(self.__DataSize) # length of the message
            timeStamp = self.read24bit(); tmpFile.write(timeStamp)             # should be 0
            unknown = self.readint(); tmpFile.write(unknown)                   # TimeStampExtended (8) + StreamID (24)
            if tagType == self.META:
                startpos = self.fp.tell();
                event = self.readAMFData(); self.metaData = self.readAMFData();
                endpos = self.fp.tell();
                self.fp.seek(self.__DataSize - (endpos-startpos),os.SEEK_CUR);   # this is a hack
            else:
                self.read(self.__DataSize)
            self.fp.seek(-(11 + self.__DataSize),os.SEEK_CUR);
            tmpFile.write(self.fp.read(11 + self.__DataSize));          # go back and read it back
            self.curDecodableHeadPos += 11 + self.__DataSize

        while self.tsp < endTime:
            tmpData = self.fp.read(4);
            ptagsize, = struct.unpack('>I', tmpData);
            if ptagsize != (self.__DataSize+11):
                if FLV_VERBOSE: print 'invalid previous tag-size found:', ptagsize, '!=', (self.__DataSize+11),'ignored.'
            bytes = self.fp.read(11); tmpData += bytes;
            type, len0, len1, ts0, ts1, ts2, sid0, sid1 = struct.unpack('>BBHBHBBH', bytes)
            self.__DataSize = (len0 << 16) | len1; ts = (ts0 << 16) | (ts1 & 0x0ffff) | (ts2 << 24)
            if self.curPos - self.curDecodableHeadPos >= 15 + self.__DataSize:
                #self.fp.seek(self.__DataSize,os.SEEK_CUR);
                tmpFile.write(tmpData);
                tmpFile.write(self.fp.read(self.__DataSize));
                self.curDecodableHeadPos += 15 + self.__DataSize             # move forward
                if ts > self.tsp:
                    self.tsp = ts
            else:
                break
        # go back to the last readable point
        self.fp.seek(self.curPos - self.fp.tell(),os.SEEK_CUR);
        tmpFile.close();
        return [self.curDecodableHeadPos,self.tsp]                           # return the next position to start, and the ending play time

    # read until exceeding self.curDecodableHeadPos is impossible
    def readtillCur(self):
        self.__lock.acquire();
        #print self.curDecodableHeadPos, self.curPos
        self.fp.seek(self.curDecodableHeadPos-self.curPos,os.SEEK_CUR)
        # read the header
        if self.curDecodableHeadPos == 0:
            magic, version, flags, offset = struct.unpack('!3sBBI', self.fp.read(9))
            self.curDecodableHeadPos += 9;
            if FLV_VERBOSE: print 'FLV.open() hdr=', magic, version, flags, offset
            if magic != 'FLV': raise ValueError('This is not a FLV file')
            if version != 1: raise ValueError('Unsupported FLV file version')
            if offset > 9:
                self.fp.seek(offset-9, os.SEEK_CUR); self.curDecodableHeadPos += offset - 9;
            self.fp.read(4); self.curDecodableHeadPos += 4;
            # read the meta data (first read 11 bytes as usual, and then read DataSize)
            tagType = self.readbyte()                          # beginning of the first tag
            self.__DataSize = self.read24bit()                 # length of the message
            timeStamp = self.read24bit()                       # should be 0
            unknown = self.readint();                          # TimeStampExtended (8) + StreamID (24)
            if tagType == self.META:
                startpos = self.fp.tell();
                event = self.readAMFData(); self.metaData = self.readAMFData();
                endpos = self.fp.tell();
                self.fp.seek(self.__DataSize - (endpos-startpos),os.SEEK_CUR);   # this is a hack
            else:
                self.fp.read(self.__DataSize)
            self.curDecodableHeadPos += 11 + self.__DataSize

        # read the body
        while self.curPos - self.curDecodableHeadPos > 15:
            ptagsize, = struct.unpack('>I', self.fp.read(4));
            if ptagsize != (self.__DataSize+11):
                if FLV_VERBOSE: print 'invalid previous tag-size found:', ptagsize, '!=', (self.__DataSize+11),'ignored.'
            bytes = self.fp.read(11);
            type, len0, len1, ts0, ts1, ts2, sid0, sid1 = struct.unpack('>BBHBHBBH', bytes)
            self.__DataSize = (len0 << 16) | len1; ts = (ts0 << 16) | (ts1 & 0x0ffff) | (ts2 << 24)
            if self.curPos - self.curDecodableHeadPos >= 15 + self.__DataSize:
                self.fp.seek(self.__DataSize,os.SEEK_CUR);
                self.curDecodableHeadPos += 15 + self.__DataSize             # move forward
                #print ts
                if ts > self.tsp:
                    self.tsp = ts
            else:
                break
        # go back to the download point
        self.fp.seek(self.curPos - self.fp.tell(),os.SEEK_CUR);
        self.__lock.release()
        return self.tsp/1000

    def get_streamingheadTime(self):
        self.__lock.acquire();
        streamingheadTime = self.tsp/1000;
        self.__lock.release();
        return streamingheadTime                                    # in seconds

    def get_Duration(self):
        if self.metaData != None:
            return self.metaData['duration']                        # get duration (secs)
        else:
            return 0;

    def get_Bitrate(self):
        if self.metaData != None:
            return (self.metaData['videodatarate']
                     + self.metaData['audiodatarate'])              # get wholistic bitrate kbps
        else:
            return 0;

    def close(self):
        if self.fp is not None: self.fp.close(); self.fp = None


    ################################### Read Meta Data ####################################

    ################################### Example of Meta Data ##############################
    #{'audiocodecid': 2.0,
    # 'audiodatarate': 62.5,
    # 'audiosamplerate': 22050.0,
    # 'audiosamplesize': 16.0,
    # 'duration': 6340.0230000000001,
    # 'filesize': 366658470.0,
    # 'framerate': 25.0,
    # 'height': 240.0,
    # 'stereo': False,
    # 'videocodecid': 2.0,
    # 'videodatarate': 390.625,
    # 'width': 320.0}
    ################################### Example of Meta Data ##############################

    def readint(self):
        data = self.fp.read(4)
        return struct.unpack('>I', data)[0]

    def readshort(self):
        data = self.fp.read(2)
        return struct.unpack('>H', data)[0]

    def readbyte(self):
        data = self.fp.read(1)
        return struct.unpack('B', data)[0]

    def read24bit(self):
        b1, b2, b3 = struct.unpack('3B', self.fp.read(3))
        return (b1 << 16) + (b2 << 8) + b3

    def readAMFData(self, dataType=None):
        if dataType is None:
            dataType = self.readbyte()
        funcs = {
            0: self.readAMFDouble,
            1: self.readAMFBoolean,
            2: self.readAMFString,
            3: self.readAMFObject,
            8: self.readAMFMixedArray,
           10: self.readAMFArray,
           11: self.readAMFDate
        }
        func = funcs[dataType]
        if callable(func):
            return func()

    def readAMFDouble(self):
        return struct.unpack('>d', self.fp.read(8))[0]

    def readAMFBoolean(self):
        return self.readbyte() == 1

    def readAMFString(self):
        size = self.readshort()
        return self.fp.read(size)

    def readAMFObject(self):
        data = self.readAMFMixedArray()
        result = object()
        result.__dict__.update(data)
        return result

    def readAMFMixedArray(self):
        size = self.readint()
        result = {}
        for i in range(size):
            key = self.readAMFString()
            dataType = self.readbyte()
            if not key and dataType == 9:
                break
            result[key] = self.readAMFData(dataType)
        return result

    def readAMFArray(self):
        size = self.readint()
        result = []
        for i in range(size):
            result.append(self.readAMFData)
        return result

    def readAMFDate(self):
        return datetime.fromtimestamp(self.readAMFDouble())

    ################################### END Read Meta Data ################################
