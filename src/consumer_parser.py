import json
import re
from typing import Dict, Any

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer


SPACE_RE = re.compile(r"\s+")
def norm_col(name: str) -> str:
    # Remove UTF-8 BOM if present
    name = name.replace("\ufeff", "")
    # Strip and collapse internal whitespace
    name = SPACE_RE.sub(" ", name.strip())
    return name

def normalize_keys(record: Dict[str, Any]) -> Dict[str, Any]:
    return {norm_col(k): v for k, v in record.items()}

NUMERIC_COLS = [
    "Flow Bytes/s", "Flow Packets/s", "Flow Duration",
    "Total Fwd Packets", "Total Backward Packets",
    "Total Length of Fwd Packets", "Total Length of Bwd Packets",
    "Fwd Packet Length Max", "Fwd Packet Length Min", "Fwd Packet Length Mean", "Fwd Packet Length Std",
    "Bwd Packet Length Max", "Bwd Packet Length Min", "Bwd Packet Length Mean", "Bwd Packet Length Std",
    "Destination Port"
]

def clean_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    rec = normalize_keys(rec)
    df = pd.DataFrame([rec])

    df = df.replace(["Infinity", "NaN", "inf", "-inf", None], pd.NA)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.fillna(0)


    return df.to_dict(orient="records")[0]


def main():
    consumer = KafkaConsumer(
        "raw.flow",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="parser-group",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Listening to raw.flow ...")
    count = 0
    try:
        for msg in consumer:
            record = clean_record(msg.value)
            dest_port = record.get("Destination Port", None)
            producer.send("norm.flow", value=record)
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} records... latest Destination Port={dest_port}")
            else:

                pass

        producer.flush()
        print(f"Done. Processed {count} records. No more messages; consumer exited cleanly.")

    except KeyboardInterrupt:
        producer.flush()
        print(f"\nStopped by user. Processed {count} records.")

if __name__ == "__main__":
    main()
