from zfec import filefec
import os, sys
from FLVWrapper import FLVWrapper

"""
Encodes a file into chunks.

Important: chunk_size: specifies size of chunk.
"""

def split_and_encode(filestr, k, n):
    # 2/25/2013 : KW Lee
    # Now, it reads 'average bitrate' from the FLV metadata, it automatically decides the size of chunk.
    buffer_size = 10 # (secs)
    flv_input = FLVWrapper(filestr, 'r+b') # FLV wrapper of the input file
    average_bit_rate = flv_input.get_Bitrate() # (Kbps)
    chunk_size = int(buffer_size * average_bit_rate * 1000 / 8) # size of each frame
    chunk_num = 1

    filename = ''
    # Remove "file-" prefix and extension.
    try:
        filename=(((filestr.split('.'))[0]).split('file-'))[1]
    except:
        print "Filename in wrong format. Must be 'file-<filename>.ext."
        return

    dirname = 'video-' + filename
    os.mkdir(dirname)

    forig = open(filestr, 'rb')
    data = forig.read(chunk_size)
    while data:
        subfilestr = filename.replace('_', '-') + '.' + str(chunk_num)
        subfile = open(dirname + '/' + subfilestr, 'wb')
        subfile.write(data)
        subfile.close()
        code(subfilestr, subfilestr, k, n, dirname)
        data = forig.read(chunk_size)
        chunk_num += 1
    forig.close()

def code(filestr, prefix, k, n, dirname=''):
    """Makes a directory for the specified file portion to code and stores
    encoded packets into that directory."""

    print "filestr", filestr, "prefix", prefix, "dirname", dirname
    current_dir = os.getcwd()
    os.chdir(dirname)
    os.mkdir(filestr + '.dir')

    f = open(filestr, 'rb')
    f.seek(0,2) # Seek to 0th bit from the end of the file (second param = 2)
    file_size = f.tell() # See what's the index of the last bit, or the filesize
    f.seek(0,0) # Seek to 0th bit of the file (second param = 0)

    # Call filefec's encode_to_files
    # parameters : File / File Size / Target Directory / File Name / k / n / File Extension
    filefec.encode_to_files(f, file_size, filestr + '.dir', prefix, k, n, '.chunk')

    os.chdir(current_dir)

if __name__ == "__main__":
    """Encodes files within the movies directory."""

    k = 20
    n = 40
    print 'sys.argv = ', sys.argv
    if len(sys.argv) < 2:
        print "Usage: python encode.py <filename> <chunk> <coded chunks>"
        print "Defaults: <chunk=20> <coded chunks=40>"
    else:
        filestr = sys.argv[1]
        if len(sys.argv) == 4:
            k = int(sys.argv[2])
            n = int(sys.argv[3])
        # movies_path = '/home/ec2-user/movies'
        # os.chdir(movies_path)
        split_and_encode(filestr, k, n)
