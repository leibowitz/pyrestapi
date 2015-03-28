import tornado.ioloop
from tornado_json import schema
from tornado_json.application import Application
from tornado_json.requesthandlers import APIHandler



class Greeting(APIHandler):

    @schema.validate(output_schema={"type":"object"})
    def get(self, name):
        """Greets you."""
        return {"name": name}

    def success(self, data):
        """When an API call is successful, the JSend object is used as a simple
        envelope for the results, using the data key.
        Override this behaviour
        """
        self.write(data)
        self.finish()


# Create and start application
application = Application(routes=[(r"/hello/(.+)", Greeting)], settings={})
application.listen(8080)
tornado.ioloop.IOLoop.instance().start()
