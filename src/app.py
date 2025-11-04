from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
import threading
import datetime
import os

app = Flask(__name__)
alerts_buffer = []            # in-memory recent alerts
MAX_ALERTS = 20               # only show latest 20 in UI
LOG_FILE = "alerts_log.json"  # persistent file for all alerts


def save_alert_to_file(alert):
    """Append alert to JSON log file for permanent storage."""
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", LOG_FILE)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(alert) + "\n")


def consume_alerts():
    """Background Kafka consumer for alerts.flow topic."""
    consumer = KafkaConsumer(
        'alerts.flow',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='flask-alert-consumer',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("✅ Flask alert listener started (alerts.flow)")
    for msg in consumer:
        alert = msg.value

        # Fill missing fields for Time, Src IP, Dst IP
        alert["timestamp"] = alert.get("timestamp") or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ctx = alert.get("context", {})
        alert["src_ip"] = ctx.get("Source IP", "N/A")
        alert["dst_ip"] = ctx.get("Destination IP", "N/A")

        # Save permanently
        save_alert_to_file(alert)

        # Maintain rolling buffer for dashboard
        alerts_buffer.append(alert)
        if len(alerts_buffer) > MAX_ALERTS:
            alerts_buffer.pop(0)


# Start background consumer thread
threading.Thread(target=consume_alerts, daemon=True).start()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/alerts")
def get_alerts():
    """Return most recent alerts in reverse order (latest first)."""
    return jsonify(list(reversed(alerts_buffer)))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
