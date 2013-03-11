import web

db = web.database(dbn='sqlite', db='tracker.db')

# SERVER
def add_server(input_ip, input_port):
    db.insert('nodes', type_of_node='server', ip=input_ip, port=input_port)

def remove_server():
    db.delete('nodes', where="type_of_node='server'", vars=locals())

def get_server():
    return db.select('nodes', where="type_of_node='server'", order='id').list()

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
