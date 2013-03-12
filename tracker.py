import web
import db_manager

urls = (
    '/', 'overview',
    '/req/(.*)', 'request',
)
app = web.application(urls, globals())

class overview:
    def GET(self):
        return_str = ''
        return_str = return_str + 'Number of online users ' + str(db_manager.get_num_of_users()) + '\n'
        return_str = return_str + 'Number of online caches ' + str(db_manager.get_num_of_caches()) + '\n'
        return_str = return_str + 'Number of videos registered ' + str(db_manager.get_num_of_videos()) + '\n'
        return return_str

class request:
    def parse_request(self, request_str):
        # REQUEST_COMMAND & ARGUMENT
        valid_req_strings = ['GET_SERVER_ADDRESS',
                            'GET_CACHES_ADDRESS',
                            'GET_ALL_VIDEOS',
                            'REGISTER_SERVER',
                            'REGISTER_VIDEO',
                            'REGISTER_USER',
                            'REGISTER_CACHE',
                            'REMOVE_SERVER',
                            'REMOVE_USER',
                            'REMOVE_CACHE']
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
            elif req_type == 'GET_CACHES_ADDRESS':
                n_of_current_caches = db_manager.get_num_of_caches()
                n_of_returned_caches = min(n_of_current_caches, int(req_arg))
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
                db_manager.add_user(arg_ip, arg_port)
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
                db_manager.remove_all_videos()
                db_manager.remove_all_nodes()
                db_manager.add_server(arg_ip, arg_port)
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

if __name__ == "__main__":
    app.run()
