import os.path
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.options
#from tornado.log import enable_pretty_logging
 
class HelloHandler(tornado.web.RequestHandler):
    def get(self, name):
        response = { 'name': name }
        self.write(response)
 
tornado.options.parse_command_line()
#enable_pretty_logging()

ssl_options = {
    "certfile": os.path.join("server.crt"),
    "keyfile": os.path.join("server.key"),
}
settings = dict(
    debug = True,
)

application = tornado.web.Application([
    (r"/hello/(.+)", HelloHandler),
], **settings)
 
if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application, ssl_options=ssl_options)
    application.listen(8080)
    http_server.listen(8443)
    tornado.ioloop.IOLoop.instance().start()
