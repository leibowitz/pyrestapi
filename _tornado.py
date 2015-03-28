import os.path
import json
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.options
#from tornado.log import enable_pretty_logging
import rethinkdb
from dougrain import Builder

class RequestHandler(tornado.web.RequestHandler):
    def initialize(self, dbconn, dbname):
        self.dbconn = dbconn
        self.dbname = dbname

    def retrieve_one_document_by_pk(self, table, pk):
        return rethinkdb.db(self.dbname).table(table).get(pk).run(self.dbconn)

    def retrieve_one_document_by_field(self, table, field, value):
        return rethinkdb.db(self.dbname).table(table).filter({field: value}).limit(1).nth(0).run(self.dbconn)
    
    def retrieve_all_documents(self, table, limit = 50, offset = 0):
        cur = rethinkdb.db(self.dbname).table(table).skip(offset).limit(limit).run(self.dbconn)
        cur.close()
        return list(cur)
 
class ObjectHandler(RequestHandler):
    def get(self, name, id):
        document = self.retrieve_one_document_by_pk(name, id)
        self.write(document)
    
    def post(self, name, id):
        response = { 
            'name': name,
            'id': id 
        }
        self.write(response)

    def put(self, name, id):
        response = { 
            'name': name,
            'id': id 
        }
        self.write(response)

    def delete(self, name, id):
        response = { 
            'name': name,
            'id': id 
        }
        self.write(response)

    def patch(self, name, id):
        response = { 
            'name': name,
            'id': id 
        }
        self.write(response)

class ListHandler(RequestHandler):
    def get(self, name):
        documents = self.retrieve_all_documents(name)
        self.write(json.dumps(documents))
    
    def post(self, name):
        response = {
            'name': name
        }
        self.write(response)
 
if __name__ == "__main__":
    tornado.options.parse_command_line()
    #enable_pretty_logging()

    ssl_options = {
        "certfile": os.path.join("server.crt"),
        "keyfile": os.path.join("server.key"),
    }
    settings = dict(
        debug = True,
    )

    conn = rethinkdb.connect('localhost', 28015)
    params = dict(
        dbconn=conn,
        dbname="test",
    )
    application = tornado.web.Application([
        (r"/(.+)/(.+)", ObjectHandler, params),
        (r"/(.+)", ListHandler, params),
    ], **settings)
 
    http_server = tornado.httpserver.HTTPServer(application, ssl_options=ssl_options)
    application.listen(8080)
    http_server.listen(8443)
    tornado.ioloop.IOLoop.instance().start()
