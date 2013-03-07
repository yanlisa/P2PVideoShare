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

