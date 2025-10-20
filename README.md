## ⚙️ 1. Environment Setup

### 🐍 Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
````

`requirements.txt`

```text
pandas
numpy
tqdm
kafka-python
pyyaml
```

### 🐳 Start Kafka with Docker Compose

```bash
cd kafka
docker-compose up -d
```

Check containers:

```bash
docker ps
```

You should see:

* `zookeeper`  – Confluent Zookeeper 7.6.0
* `kafka`      – Confluent Kafka Broker 7.6.0

---

## 🧩 2. Kafka Topic Creation

```bash
docker exec -it kafka kafka-topics \
  --create --topic raw.flow --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics \
  --create --topic norm.flow --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

## 🧠 3. Dataset Preparation

Download **CIC-IDS-2017** from [Canadian Institute for Cybersecurity](https://www.unb.ca/cic/datasets/ids-2017.html).

Create a 50 000-row sample:

```python
import pandas as pd
df = pd.read_csv('data_original/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv')
df_sample = df.sample(n=50000, random_state=42)
df_sample.to_csv('data/CICIDS2017_sample.csv', index=False)
```

---

## 🔄 4. Run Streaming Pipeline

### ▶️ Start Consumer (Parser)

```bash
python src/consumer_parser.py
```

### ▶️ Start Producer

```bash
python src/producer.py
```

### 🔍 Verify Normalized Data

```bash
docker exec -it kafka kafka-console-consumer \
  --topic norm.flow --bootstrap-server localhost:9092 \
  --from-beginning --max-messages 5
```

Expected output:

```json
{"Destination Port":80,"Flow Duration":12345.0,"Flow Bytes/s":5120.0,...}
```

---

## 🧱 5. Data Schema

`docs/data_schema.json`

```json
{
  "Destination Port": "int",
  "Flow Duration": "float",
  "Total Fwd Packets": "int",
  "Total Backward Packets": "int",
  "Flow Bytes/s": "float",
  "Flow Packets/s": "float",
  "Label": "string"
}
```

---

## 📋 6. Deliverables (Week 1)

| Deliverable              | Description                                    | Status |
| ------------------------ | ---------------------------------------------- | ------ |
| ✅ Kafka Environment      | Dockerized Zookeeper + Kafka 7.6.0             | ✔️     |
| ✅ Data Ingestion         | Producer sends CIC-IDS-2017 flows → `raw.flow` | ✔️     |
| ✅ Parser & Normalization | Consumer cleans and publishes → `norm.flow`    | ✔️     |
| ✅ Schema Documentation   | `docs/data_schema.json`                        | ✔️     |
| ✅ Test Run Verification  | Console output from `norm.flow`                | ✔️     |

---

## 🧩 7. Architecture Overview

```text
+-------------------+       +----------------+       +-----------------+
|  Python Producer  | --->  |   Kafka Topic  | --->  |  Python Consumer |
|  (CICIDS sample)  |       |   raw.flow     |       |  Normalizer →    |
|                   |       |                |       |   norm.flow      |
+-------------------+       +----------------+       +-----------------+
```

---

## ☁️ 8. Next Steps (Week 2 Preview)

* Implement **feature engineering** on normalized flows.
* Train an **anomaly detection model** (Isolation Forest / LSTM Autoencoder).
* Stream scores to an **alerts topic**.

---

## 🧰 Troubleshooting

| Issue                                | Possible Cause                        | Fix                                              |
| ------------------------------------ | ------------------------------------- | ------------------------------------------------ |
| `FileNotFoundError`                  | Wrong dataset path                    | Confirm file under `data_original/`              |
| `None` values for `Destination Port` | Leading spaces in column names        | Use normalized consumer_parser.py (key trimming) |
| `KeyboardInterrupt`                  | Manual stop of infinite consumer loop | Expected; consumer listens continuously          |
| Broker connection error              | Kafka container not running           | Run `docker-compose up -d` again                 |

---

## 🧾 References

* [CIC IDS 2017 Dataset](https://www.unb.ca/cic/datasets/ids-2017.html)
* [Confluent Kafka Docker Images](https://hub.docker.com/r/confluentinc/cp-kafka)
* [Kafka Python Client Documentation](https://kafka-python.readthedocs.io/en/master/)

---

**Author:** *Praneeth Krishna Palle*
**Environment:** PyCharm + venv + Docker Compose + Confluent Kafka 7.6.0
**Date:** Week 1 Implementation — Infrastructure & Data Ingestion

```

---

Would you like me to auto-generate the accompanying **`requirements.txt`** and **`.env`** file (so this README fully matches your runnable project)?
```
