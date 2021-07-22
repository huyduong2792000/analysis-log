from flask import Flask

app = Flask(__name__)
#  /var/log/nginx/nginx_error.log
@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

# if __name__ == '__main__':
#     app.run(host = "0.0.0.0", port=8000)