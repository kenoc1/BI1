from flask import Flask, render_template, url_for, session, request, jsonify

app = Flask(__name__)
app.debug = True


@app.route("/")
def index():
    return render_template('index.html')


if __name__ == "__main__":
    app.run(host='localhost', port=8080, threaded=True)
