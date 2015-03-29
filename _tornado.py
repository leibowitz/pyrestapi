import os.path
import json
import traceback
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.options
#from tornado.log import enable_pretty_logging
import rethinkdb
from dougrain import Builder

class ObjectURLRequestHandler():
    def resource_object_url(self, name, pk):
        return "/{}/{}".format(name, pk)
    
    def resource_list_url(self, name):
        return "/{}".format(name)

class JSONRequestHandler(tornado.web.RequestHandler):
    def write_error(self, status_code, **kwargs):
        if self.settings.get("serve_traceback") and "exc_info" in kwargs:
            # in debug mode, try to send a traceback
            self.set_header('Content-Type', 'application/json')
            for line in traceback.format_exception(*kwargs["exc_info"]):
                self.write({"error": line})
            self.finish()
        else:
            self.finish({
                            "code": status_code,
                            "message": self._reason,
                        })

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
                message = 'Unable to parse JSON.'
                self.send_error(400, message=message) # Bad Request

class DBRequestHandler(tornado.web.RequestHandler):
    def initialize(self, dbconn, dbname):
        self.dbconn = dbconn
        self.dbname = dbname

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
    
    def retrieve_all_documents(self, table, limit = 0, offset = 0, fields = []):
        q = rethinkdb\
            .db(self.dbname)\
            .table(table)

        if len(fields) != 0:
            q = q.pluck(fields)

        if offset != 0:
            q = q.skip(offset)

        if limit != 0:
            q = q.limit(limit)

        cur = q.run(self.dbconn)
        cur.close()
        return list(cur)

    def insert_document(self, table, document):
        res = rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .insert(document)\
            .run(self.dbconn)

        if res['inserted'] != 0 and len(res['generated_keys']) != 0:
            document['id'] = res['generated_keys'][0]
            return document

        return None

    def update_document_by_pk(self, table, pk, document):
        if 'id' not in document:
            document['id'] = pk

        res = rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .get(pk)\
            .update(document)\
            .run(self.dbconn)

        if res['replaced'] != 0:
            return True
        if res['unchanged'] != 0:
            return False

        return None

    def replace_document_by_pk(self, table, pk, document):
        if 'id' not in document:
            document['id'] = pk

        res = rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .get(pk)\
            .replace(document)\
            .run(self.dbconn)

        if res['replaced'] != 0:
            return True
        if res['unchanged'] != 0:
            return False

        return None

    def delete_document_by_pk(self, table, pk):
        res = rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .get(pk)\
            .delete()\
            .run(self.dbconn)

        if res['deleted'] != 0:
            return True

        return False
       
    def count_documents(self, table):
        return rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .count()\
            .run(self.dbconn)

class JSONORMRestAPIRequestHandler(JSONRequestHandler, ObjectURLRequestHandler, DBRequestHandler):
    pass

class ObjectHandler(JSONORMRestAPIRequestHandler):
    def get(self, name, pk):
        document = self.retrieve_one_document_by_pk(name, pk)
        if document is None:
            self.send_error(404)
            return
        self.write(document)
    
    def post(self, name, pk):
        document = self.request.json_data
        op = self.update_document_by_pk(name, pk, document)
        if op is None:
            self.send_error(404)
            return
        self.write(document)

    def patch(self, name, pk):
        document = self.request.json_data
        op = self.update_document_by_pk(name, pk, document)
        if op is None:
            self.send_error(404)
            return
            
        self.write(document)

    def put(self, name, pk):
        document = self.request.json_data
        op = self.replace_document_by_pk(name, pk, document)
        if op is None:
            self.send_error(404)
            return
            
        self.write(document)

    def delete(self, name, pk):
        if not self.delete_document_by_pk(name, pk):
            self.send_error(404)
            return

        self.set_status(410)
        self.finish()

class ListHandler(JSONORMRestAPIRequestHandler):
    def get(self, name):
        limit = self.get_argument('limit', 50)
        offset = self.get_argument('offset', 0)
        fields = self.get_arguments('field', [])
        documents = self.retrieve_all_documents(name, limit, offset, fields)
        self.write(json.dumps(documents))

    def head(self, name):
        nr = self.count_documents(name)
        self.set_header("X-Result-Count", nr)
        self.finish()

    def post(self, name):
        document = self.request.json_data
        res = self.insert_document(name, document)
        if res is None:
            self.send_error(400)
            return
        self.set_status(201)
        self.set_header("Location", self.resource_object_url(name, res['id']))
        self.write(res)
 
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
