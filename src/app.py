from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
import threading

app = Flask(__name__)
alerts_buffer = []         # stores recent alerts
MAX_ALERTS = 50

def consume_alerts():
    consumer = KafkaConsumer(
        'alerts.flow',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    print("Flask alert listener started (alerts.flow)")
    for msg in consumer:
        alert = msg.value
        alerts_buffer.append(alert)
        if len(alerts_buffer) > MAX_ALERTS:
            alerts_buffer.pop(0)

# background consumer thread
threading.Thread(target=consume_alerts, daemon=True).start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/alerts")
def get_alerts():
    return jsonify(list(reversed(alerts_buffer)))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
