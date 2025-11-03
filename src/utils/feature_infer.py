import json
import joblib
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from feature_engineer import build_feature_vector, TARGET_COLS


def main():
    try:
        model = joblib.load('model/isolation_forest.joblib')
        print("Isolation Forest model loaded successfully.")
    except Exception as e:
        print(f"Failed to load model: {e}")
        return

    try:
        consumer = KafkaConsumer(
            'norm.flow',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',      # start from beginning
            enable_auto_commit=True,
            group_id='feature-infer',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        print("Connected to Kafka. Listening on topic: norm.flow ...")
        print("Scoring events in real time...\n")

    except Exception as e:
        print(f"Kafka connection failed: {e}")
        return

    threshold = -0.15
    count = 0

    try:
        for msg in consumer:
            record = msg.value
            if not isinstance(record, dict):
                continue

            features = pd.DataFrame(
                [build_feature_vector(record)],
                columns=TARGET_COLS
            )

            score = float(model.decision_function(features)[0])
            is_anomaly = bool(score < threshold)

            alert = {
                "timestamp": record.get("Timestamp", ""),
                "score": float(score),
                "threshold": float(threshold),
                "is_anomaly": is_anomaly,
                "context": {
                    "Source IP": record.get("Source IP", ""),
                    "Destination IP": record.get("Destination IP", ""),
                    "Destination Port": record.get("Destination Port", "")
                }
            }

            try:
                producer.send('alerts.flow', value=alert)
            except Exception as e:
                print(f"Failed to send alert: {e}")

            count += 1
            if count % 100 == 0:
                print(f"[{count}] score={score:.3f} anomaly={is_anomaly}")

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        print(f"Error during inference: {e}")
    finally:
        producer.flush()
        print(f"\nCompleted streaming inference. Total records processed: {count}")


if __name__ == "__main__":
    main()
