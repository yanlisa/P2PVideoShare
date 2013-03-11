import web
import db_manager

urls = (
    '/req/(.*)', 'request',
)
app = web.application(urls, globals())

class request:
    def parse_request(self, request_str):
        # REQUEST_COMMAND & ARGUMENT
        valid_req_strings = ['GET_SERVER_ADDRESS', 'GET_CACHES_ADDRESS', 'REGISTER_USER', 'REGISTER_CACHE', 'REGISTER_SERVER']
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
                return 'User is registered'
            elif req_type == 'REGISTER_SERVER':
                arg_ip = req_arg.split('_')[0]
                arg_port = req_arg.split('_')[1]
                db_manager.add_server(arg_ip, arg_port)
                return 'User is registered'

if __name__ == "__main__":
    app.run()
