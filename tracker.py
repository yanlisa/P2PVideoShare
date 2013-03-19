import ast
import web
import db_manager

urls = (
    '/', 'overview',
    '/req/(.*)', 'request',
)
app = web.application(urls, globals())
render = web.template.render('templates/')

class overview:
    def GET(self):
        nodes_info = db_manager.get_all_nodes()
        videos_info = db_manager.get_all_videos()
        nodes_info2 = []
        videos_info2 = []

        n_nodes = [0, 0, 0] # Server / cache / user

        # Convert 'chunk indexes' to ints
        for each in nodes_info:
            each.stored_chunks = ast.literal_eval(str(each.stored_chunks))
            if each.stored_chunks is not None:
                if len(each.stored_chunks.keys()) == 0:
                    continue
                for key, val in each.stored_chunks.items():
                    stored_chunk_str = str(val)
                    stored_chunk_list = map(int, ast.literal_eval(stored_chunk_str))
                    val = stored_chunk_list.sort()
                    each.stored_chunks[key] = stored_chunk_list

        # Convert storages to lists
        for each in nodes_info:
            nodes_info2.append([each.id, str(each.type_of_node), str(each.ip), str(each.port), str(each.watching_video), each.stored_chunks])
            #nodes_info2.append([each.id, str(each.type_of_node), str(each.ip), str(each.port), str(each.watching_video), ast.literal_eval(str(each.stored_chunks))])
            if str(each.type_of_node) == 'server':
                n_nodes[0] = n_nodes[0] + 1
            elif str(each.type_of_node) == 'cache':
                n_nodes[1] = n_nodes[1] + 1
            elif str(each.type_of_node) == 'user':
                n_nodes[2] = n_nodes[2] + 1
        for each in videos_info:
            videos_info2.append([each.id, str(each.vname), each.n_of_frames, each.code_param_n, each.code_param_k, each.total_size, each.chunk_size, each.last_chunk_size])

        print '[tracker.py] nodes_info ', nodes_info2
        print '[tracker.py] n_nodes ', n_nodes
        print '[tracker.py] videos_info ', videos_info2

        return render.overview(nodes_info2, n_nodes, videos_info2)

class request:
    def parse_request(self, request_str):
        # REQUEST_COMMAND & ARGUMENT
        valid_req_strings = ['GET_SERVER_ADDRESS',
                            'GET_SERVER_ADDRESS_FOR_CACHE',
                            'GET_CACHES_ADDRESS',
                            'GET_ALL_VIDEOS',
                            'UPDATE_CHUNKS_FOR_CACHE',
                            'REGISTER_SERVER',
                            'REGISTER_SERVER_FOR_CACHE',
                            'REGISTER_VIDEO',
                            'REGISTER_USER',
                            'REGISTER_CACHE',
                            'REMOVE_SERVER',
                            'REMOVE_SERVER_FOR_CACHE',
                            'REMOVE_USER',
                            'REMOVE_CACHE',
                            'UPDATE_SERVER_LOAD']
        req_type = request_str.split('&')[0]
        if len(request_str.split('&')) > 1:
            req_arg = request_str.split('&')[1]
        else:
            req_arg = 0
        req_valid = req_type in valid_req_strings
        return req_valid, req_type, req_arg

    def GET(self, request_str):
        req_valid, req_type, req_arg = self.parse_request(request_str)
        if req_valid == False:
            return 'Invalid request'
        else:
            # REQUEST NODE INFO
            if req_type == 'GET_SERVER_ADDRESS':
                res = db_manager.get_server()
                return str(res[0].ip) + ' ' + str(res[0].port)
            elif req_type == 'GET_SERVER_ADDRESS_FOR_CACHE':
                print 'get_server_address_for_cache'
                res = db_manager.get_server_for_cache()
                return str(res[0].ip) + ' ' + str(res[0].port)
            elif req_type == 'GET_CACHES_ADDRESS':
                n_of_current_caches = db_manager.get_num_of_caches()
                n_of_returned_caches = min(n_of_current_caches, int(req_arg))
                print '[tracker.py] n_of_returned_caches', n_of_returned_caches
                caches = db_manager.get_many_caches(n_of_returned_caches)
                ret_str = ''
                for cache in caches:
                    ret_str = ret_str + str(cache.ip) + ' ' + str(cache.port) + '\n'
                return ret_str
            # NODE REGISTER
            elif req_type == 'REGISTER_USER':
                # req_arg = "143.243.23.13_324"
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                arg_watching_video = req_arg.split('_')[2]
                db_manager.add_user(arg_ip, arg_port, arg_watching_video)
                return 'User is registered'
            elif req_type == 'REGISTER_CACHE':
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                db_manager.add_cache(arg_ip, arg_port)
                return 'Cache is registered'
            elif req_type == 'REGISTER_SERVER':
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                # remove existing server & videos
                db_manager.remove_server()
                db_manager.remove_server_for_cache()
                db_manager.remove_all_videos()
                db_manager.remove_all_nodes()
                db_manager.add_server(arg_ip, arg_port)
                return 'Server is registered'
            elif req_type == 'REGISTER_SERVER_FOR_CACHE':
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                # remove existing server & videos
                db_manager.add_server_for_cache(arg_ip, arg_port)
                return 'Server is registered'
            # VIDEO REGISTER
            elif req_type == 'REGISTER_VIDEO':
                print 'add video'
                split_arg = req_arg.split('_')
                arg_vname = split_arg[0]
                arg_n_of_frames = split_arg[1]
                arg_code_param_n = split_arg[2]
                arg_code_param_k = split_arg[3]
                arg_total_size = split_arg[4]
                arg_chunk_size = split_arg[5]
                arg_last_chunk_size = split_arg[6]
                db_manager.add_video(arg_vname, arg_n_of_frames, arg_code_param_n, arg_code_param_k, arg_total_size, arg_chunk_size, arg_last_chunk_size)
                return 'Video is registered'
            elif req_type == 'GET_ALL_VIDEOS':
                videos = db_manager.get_all_videos()
                ret_str = ''
                for video in videos:
                    ret_str = ret_str + str(video.id) + ' ' + str(video.vname) + ' ' + str(video.n_of_frames) + ' ' + str(video.code_param_n) + ' ' + str(video.code_param_k) + ' ' + str(video.total_size) + ' ' + str(video.chunk_size) + ' ' + str(video.last_chunk_size) + '\n'
                return ret_str
            elif req_type == 'REMOVE_SERVER':
                db_manager.remove_server()
                return 'Server is removed'
            elif req_type == 'REMOVE_SERVER_FOR_CACHE':
                db_manager.remove_server_for_cache()
                return 'Server for cache is removed'
            elif req_type == 'REMOVE_USER':
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                db_manager.remove_user(arg_ip, arg_port)
                return 'User is removed'
            elif req_type == 'REMOVE_CACHE':
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                db_manager.remove_cache(arg_ip, arg_port)
                return 'Cache is removed'
            elif req_type == 'UPDATE_CHUNKS_FOR_CACHE':
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                arg_vname = req_arg.split('_')[2]
                arg_chunk_str = req_arg.split('_')[3]
                db_manager.add_chunks_for_cache(arg_ip, arg_port, arg_vname, arg_chunk_str)
            elif req_type == 'UPDATE_SERVER_LOAD':
                arg_vname = req_arg.split('_')[0]
                arg_n_of_chks = req_arg.split('_')[1]
                db_manager.add_server_load(arg_vname, arg_n_of_chks)


if __name__ == "__main__":
    app.run()
