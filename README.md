# BI1

Die Scripte zum Befüllen der Filiale 2 Datenbank befinden sich im Ordner F2. Die Script zum Befüllen der integrierten Datenbank befindet sich im Ordner combined. 

Die Dateien db_service.py und util.py sowie der Ordner data werden für die beiden genannten Arten von Scripten genutzt.

Die Datei config.py enthält Konstanten sowie lädt Geheimnisse für den Verbindungsaufbau.

Die Datei db_service.py enthält eine Klasse DB mit der jegliche Kommunikation mit der Datenbank abgewickelt wurde.

Die Datei util.py enthält Methoden für umrechnungen sowie für die Erzeugung von Daten.

Das terminal.py ist eine Anwendung die ein Terminal in der Filiale 2 simuliert. Es befindet sich im Ordner F2.

Der Ordner rudolf enthält eine Applikation, die einen kontinuierlichen und automatisch startbaren ETL-Prozess abwickeln kann. Alle dafür benötigten Dateien sind in diesem Ordner enthalten. Die Hauptklasse ist app.py.



##Installation
.env Datei mit Secrets erstellen
app.py starten und url{localhost:5000} aufrufen
