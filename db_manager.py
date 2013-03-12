import web

db = web.database(dbn='sqlite', db='tracker.db')

# SERVER
def add_server(input_ip, input_port):
    db.insert('nodes', type_of_node='server', ip=input_ip, port=input_port)

def remove_server():
    db.delete('nodes', where="type_of_node='server'", vars=locals())

def get_server():
    return db.select('nodes', where="type_of_node='server'", order='id').list()

# VIDEO
def add_video(input_vname, input_n_of_frames, input_code_param_n, input_code_param_k, input_total_size, input_chunk_size, input_last_chunk_size):
    db.insert('videos',
            vname = input_vname,
            n_of_frames = input_n_of_frames,
            code_param_n = input_code_param_n,
            code_param_k = input_code_param_k,
            total_size = input_total_size,
            chunk_size = input_chunk_size,
            last_chunk_size = input_last_chunk_size)

def remove_video(input_vname):
    db.delete('videos', where="vname=$input_vname", vars=locals())

def remove_all_videos():
    db.delete('videos', where="id>=0", vars=locals())

def get_video(input_vname):
    return db.select('videos', where="vname=$input_vname", vars=locals()).list()

def get_all_videos():
    return db.select('videos').list()

def get_num_of_videos():
    results = db.query("SELECT count(*) AS ct FROM videos")
    return results[0].ct

# NODE
def remove_all_nodes():
    db.delete('nodes', where="id>=0", vars=locals())

# CACHE
def add_cache(input_ip, input_port):
    db.insert('nodes', type_of_node='cache', ip=input_ip, port=input_port)

def remove_cache(input_ip, input_port):
    db.delete('nodes', where="type_of_node='cache' AND ip=$input_ip AND port=$input_port", vars=locals())

def get_cache(node_id):
    return db.select('nodes', where="type_of_node='cache' AND id=$node_id", order='id', vars=locals()).list()

def get_many_caches(num_of_caches):
    return db.query("SELECT * FROM nodes WHERE type_of_node='cache' ORDER BY RANDOM() LIMIT %d" % num_of_caches).list()

def get_num_of_caches():
    results = db.query("SELECT count(*) AS ct FROM nodes WHERE type_of_node='cache'")
    return results[0].ct

# USER
def add_user(input_ip, input_port):
    db.insert('nodes', type_of_node='user', ip=input_ip, port=input_port)

def remove_user(input_ip, input_port):
    db.delete('nodes', where="type_of_node='user' AND ip=$input_ip AND port=$input_port", vars=locals())

def get_user(node_id):
    return db.select('nodes', where="type_of_node='user' AND id=$node_id", order='id').list()

def get_num_of_users():
    results = db.query("SELECT count(*) AS ct FROM nodes WHERE type_of_node='user'")
    return results[0].ct
