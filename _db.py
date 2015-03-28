import json

from dougrain import Builder
import rethinkdb as r

r.connect('localhost', 28015).repl()
document = r.db("test").table("authors").limit(1).nth(0).run()
#print document
author = Builder("/authors/"+document['id'])
#author.add_curie('admin', "/adminrels/{rel}")

for k, v in document.iteritems():
    author.set_property(k, v)

print(json.dumps(author.as_object(), indent=2))

