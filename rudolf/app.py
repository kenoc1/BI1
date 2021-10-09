from flask import Flask, render_template
from flask_socketio import SocketIO

from etl_pipeline import ETLPipeline

app = Flask(__name__)
app.debug = True
socketio = SocketIO(app)
etl_pipeline = ETLPipeline()


@app.route("/", methods=["GET"])
def index():
    # etl_pipeline.connect()
    # thread = threading.Thread(target=etl_pipeline.test(), daemon=True)
    # thread.start()
    return render_template('index.html', progress=[])


@socketio.on('start_adresse')
def on_update_progress():
    etl_pipeline.start_adresse()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_funktion')
def on_update_progress():
    etl_pipeline.start_funktion()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_mitarbeiter')
def on_update_progress():
    etl_pipeline.start_mitarbeiter()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_kunde')
def on_update_progress():
    etl_pipeline.start_kunde()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_marke')
def on_update_progress():
    etl_pipeline.start_marke()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_produkt')
def on_update_progress():
    etl_pipeline.start_produkt()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_preishistorie')
def on_update_progress():
    etl_pipeline.start_preishistorie()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_lagerplatz')
def on_update_progress():
    etl_pipeline.start_lagerplatz()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_zwhaendler')
def on_update_progress():
    etl_pipeline.start_zwhaendler()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_einkauf')
def on_update_progress():
    etl_pipeline.start_einkauf()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


@socketio.on('start_bestellung')
def on_update_progress():
    etl_pipeline.start_bestellung()
    return render_template('progress.html', progress=etl_pipeline.progress_list)


if __name__ == "__main__":
    socketio.run(app)
