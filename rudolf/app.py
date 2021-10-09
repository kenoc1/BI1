from socket import SocketIO

from flask import Flask, render_template

from etl_pipeline import ETLPipeline

app = Flask(__name__)
app.debug = True
socketio = SocketIO(app)
etl_pipeline = ETLPipeline()


@app.route("/", methods=["GET"])
def index():
    etl_pipeline.connect()
    etl_pipeline.test()
    return render_template('index.html', progress=[{"name": "Adressen", "status": 0}])


@app.route("/", methods=["POST"])
def start():
    # ETL-Pipeline starten -> wenn fertig: sio.emit('my message', [])
    return render_template('index.html', progress=[])


@socketio.on('update_progress')
def on_update_progress(data):
    print(data)
    # progress-array.append(data)
    # return render_template('index.html', progress=[])


if __name__ == "__main__":
    socketio.run(app)
    app.run(host='localhost', port=8080, threaded=True)
