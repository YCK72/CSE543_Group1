import json, time, pandas as pd
from kafka import KafkaProducer

def get_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def main():
    df = pd.read_csv('data_processed/CICIDS2017_processed.csv')
    producer = get_producer()

    for _, row in df.iterrows():
        producer.send('raw.flow', value=row.to_dict())
        time.sleep(0.001)  # mimic live stream
    producer.flush()
    print("Published data to raw.flow")

if __name__ == "__main__":
    main()
