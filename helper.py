import os

def chunk_exists_in_frame_dir(folder_name, chunk_index):
    # returns True if the chunk exists
    chunksNums = []
    try:
        for chunk_name in os.listdir(folder_name):
            chunk_suffix = (chunk_name.split('_')[0].split('.'))[-1]
            if chunk_suffix.isdigit():
                if int(chunk_suffix) == int(chunk_index[0]):
                    return True
        return False
    except:
        return False

def chunk_nums_in_frame_dir(folder_name):
    # returns an array of chunk numbers (ints) in this frame.
    # folder_name ends in '/'.
    # assumes chunk filenames end in chunk number.
    chunksNums = []
    for chunk_name in os.listdir(folder_name):
        chunk_suffix = (chunk_name.split('_')[0].split('.'))[-1]
        if chunk_suffix.isdigit():
            chunksNums.append(chunk_suffix)
    return chunksNums

def chunk_files_in_frame_dir(folder_name):
    # opens file objects for each file in the directory.
    # folder_name ends in '/'.
    chunksList = []
    for chunk_name in os.listdir(folder_name):
        chunkFile = open(folder_name + chunk_name, 'rb')
        chunksList.append(chunkFile)
    return chunksList


def parse_chunks(arg):
    """Returns file name, chunks, and frame number.
    File string format:
        file-<filename>.<framenum>.<chunk1>%<chunk2>%<chunk3>&binary_g

    """
    filestr = arg.split('&')[0]
    binarystr = arg.split('&')[1]
    if filestr.find('file-') != -1:
        filestr = (filestr.split('file-'))[-1]

    parts = filestr.split('.')
    if len(parts) < 2:
            return None
    filename, framenum = parts[0], parts[1]
    rettuple = (filename, framenum, int(binarystr))
    if len(parts[2]) == 0:
        return (filename, framenum, int(binarystr), [])
    else:
        chunks = map(int, (parts[2]).split('%'))
        return (filename, framenum, int(binarystr), chunks)

