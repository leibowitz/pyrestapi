import os.path
import json
import itertools
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

class UtilsRequestHandler(tornado.web.RequestHandler):
    def get_int_argument(self, name, default=[], strip=True):
        v = self.get_argument(name, default, strip)
        try:
            v = int(v)
        except ValueError:
            return None
        return v

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

    def large_int(self, value):
        if value > 2147483647:
            return True
        return False

    def retrieve_one_document_by_pk(self, table, pk, with_fields = [], without_fields = []):
        q = rethinkdb\
                .db(self.dbname)\
                .table(table)\
                .get(pk)

        if len(with_fields) != 0:
            q = q.pluck(with_fields)
        
        if len(without_fields) != 0:
            q = q.without(without_fields)

        return q.run(self.dbconn)

    def retrieve_one_document_by_field(self, table, field, value):
        q = rethinkdb\
            .db(self.dbname)\
            .table(table)\
            .filter({field: value})\
            .limit(1)\
            .nth(0)
        return q.run(self.dbconn)

    def sort_fields(self, sort_fields=[]):
        for k, name in enumerate(sort_fields):
            if name[0] == "-":
                sort_fields[k] = rethinkdb.desc(name[1:])
            if name[1] == "+":
                sort_fields[k] = name[1:]
        return sort_fields

    def ordered_deps(self, join_fields = [], table = None):

        if table is not None:
            # get the list of table names we want to query
            #tables = [x[0]['table'] for x in join_fields if len(x) != 0 and 'table' in x[0]]
            tables = [(x['table'], y['table']) for (x, y) in join_fields]
            tables = set(itertools.chain.from_iterable(tables))

            if table not in tables:
                return None

        name_to_instance = dict( (unicode(join[1]['table']), join) for join in join_fields) 
        name_to_deps = dict( (unicode(join[1]['table']), set([unicode(join[0]['table'])])) for join in join_fields) 

        ordered_fields = []

        if table is not None:
            # remove table dependencies
            for deps in name_to_deps.itervalues():
                if table in deps:
                    deps.remove(table)
            #name_to_deps[unicode(table)] = set()

        while name_to_deps: 
            # get all tables where all dependencies have been solved
            ready = {name for name, deps in name_to_deps.iteritems() if not deps}

            # If there aren't any, we have a problem
            if not ready:
                return None

            for name in ready:
                # remove them from dependencies
                del name_to_deps[name]

            for deps in name_to_deps.itervalues():
                deps.difference_update(ready)

            ordered_fields.extend([name_to_instance[name] for name in ready])
        
        return ordered_fields
    
    def retrieve_all_documents(self, table, limit = 0, offset = 0, with_fields = [], without_fields = [], sort_fields = [], join_fields = []):
        q = rethinkdb\
            .db(self.dbname)\
            .table(table)

        if len(with_fields) != 0:
            q = q.pluck(with_fields)
        
        if len(without_fields) != 0:
            q = q.without(without_fields)

        if len(sort_fields) != 0:
            sort_fields = self.sort_fields(sort_fields)
            q = q.order_by(*sort_fields)

        if offset != 0:
            if self.large_int(offset):
                return None
            q = q.skip(offset)

        if limit != 0:
            if self.large_int(limit):
                return None
            q = q.limit(limit)

        if len(join_fields) != 0:
            print join_fields
            q = q.map(
                        rethinkdb.row.merge(lambda doc: {
                            table: doc
                        })
                        .pluck(table)
                    )
            
            for fields in join_fields:
                if len(fields) != 2:
                    return None

                left, right = fields

                q = q.eq_join(
                        rethinkdb.row[left['table']][left['field']], 
                        rethinkdb.table(right['table']), 
                        index=right['field'])\
                    .map(
                        rethinkdb.row.merge(lambda doc: doc["left"])
                        .without("left")
                    )\
                    .map(
                        rethinkdb.row.merge(lambda doc: {
                            right['table']: doc["right"]
                        })
                        .without("right")
                    )

            q = q.map(
                rethinkdb.row.merge(lambda doc: doc[table])
                .without(table)
            )

        cur = q.run(self.dbconn)
        if type(cur) == list:
            return cur
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

class JSONORMRestAPIRequestHandler(JSONRequestHandler, ObjectURLRequestHandler, DBRequestHandler, UtilsRequestHandler):

    def parse_fields_filter(self, fields):
        with_fields = []
        without_fields = []
        for name in fields:
            if name[0] == "-":
                without_fields.append(name[1:])
            elif name[0] == "+":
                with_fields.append(name[1:])
            else:
                with_fields.append(name)
        return with_fields, without_fields

    def parse_table_field(self, field, table=None):
        if '.' in field:
            args = field.split('.')
            if len(args) == 2:
                table, field = args 
            elif len(args) == 1:
                field = args
            else:
                field = None
                
        return table, field

    def parse_join_fields(self, name, exclude = []):
        join_fields = []

        args = self.request.query_arguments.copy()

        for name in exclude:
            if name in args:
                del args[name]

        for k, values in args.iteritems():
            table, field = self.parse_table_field(k, name)
            if field is None:
                return None
            mapping = [{'table': table, 'field': field}]
            for v in values:
                table, field = self.parse_table_field(v, name)
                if field is None:
                    return None

                cond = mapping[:]
                cond.append({'table': table, 'field': field})

                join_fields.append(cond)
        return join_fields

class ErrorHandler(tornado.web.ErrorHandler, JSONRequestHandler):
    pass

class ObjectHandler(JSONORMRestAPIRequestHandler):

    def get(self, name, pk):
        fields = self.get_arguments('field', [])
        with_fields, without_fields = self.parse_fields_filter(fields)

        document = self.retrieve_one_document_by_pk(name, pk, with_fields, without_fields)
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
        limit = self.get_int_argument('limit', 50)
        if limit is None or limit <= 0:
            self.send_error(400)
            return
        offset = self.get_int_argument('offset', 0)
        if offset is None or offset < 0:
            self.send_error(400)
            return
            
        fields = self.get_arguments('field', [])
        with_fields, without_fields = self.parse_fields_filter(fields)
        
        sort_fields = self.get_arguments('sort', [])

        join_fields = self.parse_join_fields(name, ['field', 'offset', 'limit', 'sort'])
        if join_fields is None:
            self.send_error(400)
            return

        documents = self.retrieve_all_documents(name, limit, offset, with_fields, without_fields, sort_fields, join_fields)
        if documents is None:
            self.send_error(400)
            return
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
        default_handler_class = ErrorHandler,
        default_handler_args = dict(status_code=404),
    )

    conn = rethinkdb.connect('localhost', 28015)
    params = dict(
        dbconn=conn,
        dbname="test",
    )
    application = tornado.web.Application([
        (r"/(?P<name>[^/]+)/(?P<pk>[^/]+)", ObjectHandler, params),
        (r"/(?P<name>[^/]+)", ListHandler, params),
    ], **settings)
 
    http_server = tornado.httpserver.HTTPServer(application, ssl_options=ssl_options)
    application.listen(8080)
    http_server.listen(8443)
    tornado.ioloop.IOLoop.instance().start()
