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

    def prepare(self):
        '''Incorporate request JSON into arguments dictionary.'''
        #if self.request.headers["Content-Type"].startswith("application/json"):
        if self.request.method in ("POST", "PUT", "PATCH"):
            if not self.request.body:
                self.request.json_data = dict()
                return

            try:
                self.request.json_data = json.loads(self.request.body)
                json_data = dict()
                for k, v in self.request.json_data.items():
                    # Tornado expects values in the argument dict to be lists.
                    # in tornado.web.RequestHandler._get_argument the last argument is returned.
                    json_data[k] = [v]
                self.request.arguments.pop(self.request.body)
                self.request.arguments.update(json_data)
            except ValueError, e:
                logger.debug(e.message)
                message = 'Unable to parse JSON.'
                self.send_error(400, message=message) # Bad Request


    def retrieve_one_document_by_pk(self, table, pk):
        return rethinkdb\
                .db(self.dbname)\
                .table(table)\
                .get(pk)\
                .run(self.dbconn)

    def retrieve_one_document_by_field(self, table, field, value):
        return rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .filter({field: value})\
            .limit(1)\
            .nth(0)\
            .run(self.dbconn)
    
    def retrieve_all_documents(self, table, limit = 50, offset = 0):
        cur = rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .skip(offset)\
            .limit(limit)\
            .run(self.dbconn)
        cur.close()
        return list(cur)

    def insert_document(self, table, document):
        return rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .insert(document)\
            .run(self.dbconn)

    def update_document_by_pk(self, table, pk, document):
        if 'id' not in document:
            document['id'] = pk
        return rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .get(pk)\
            .update(document)\
            .run(self.dbconn)

    def replace_document_by_pk(self, table, pk, document):
        if 'id' not in document:
            document['id'] = pk
        return rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .get(pk)\
            .replace(document)\
            .run(self.dbconn)

    def delete_document_by_pk(self, table, pk):
        return rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .get(pk)\
            .delete()\
            .run(self.dbconn)
       
class ObjectHandler(RequestHandler):
    def get(self, name, pk):
        document = self.retrieve_one_document_by_pk(name, pk)
        if document is None:
            self.send_error(404)
            return
        self.write(document)
    
    def post(self, name, pk):
        document = self.request.json_data
        self.update_document_by_pk(name, pk, document)
        self.write(document)

    def patch(self, name, pk):
        document = self.request.json_data
        self.update_document_by_pk(name, pk, document)
        self.write(document)

    def put(self, name, pk):
        document = self.request.json_data
        self.replace_document_by_pk(name, pk, document)
        self.write(document)

    def delete(self, name, pk):
        self.delete_document_by_pk(name, pk)
        self.write({'id': pk})

class ListHandler(RequestHandler):
    def get(self, name):
        documents = self.retrieve_all_documents(name)
        self.write(json.dumps(documents))

    def post(self, name):
        document = self.request.json_data
        self.insert_document(name, document)
        self.write(document)
 
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
