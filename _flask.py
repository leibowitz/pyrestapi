from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/hello/<string:name>')
def api_hello(name):
    return jsonify({'name': name})

if __name__ == '__main__':
    app.run(debug=True, port=8080)
