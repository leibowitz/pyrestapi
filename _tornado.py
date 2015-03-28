import os.path
import tornado.ioloop
import tornado.web
import tornado.httpserver
 
class HelloHandler(tornado.web.RequestHandler):
    def get(self, name):
        response = { 'name': name }
        self.write(response)
 
application = tornado.web.Application([
    (r"/hello/(.+)", HelloHandler),
])
 
if __name__ == "__main__":
    settings = dict(
        ssl_options = {
            "certfile": os.path.join("server.crt"),
            "keyfile": os.path.join("server.key"),
        }
    )
    http_server = tornado.httpserver.HTTPServer(application, **settings)
    application.listen(8080)
    http_server.listen(8443)
    tornado.ioloop.IOLoop.instance().start()
