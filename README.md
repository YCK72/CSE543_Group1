1. Environment Setup
Create Virtual Environment
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt

requirements.txt
pandas
numpy
tqdm
kafka-python
scikit-learn
joblib
flask
matplotlib
shap
pyyaml

Start Kafka with Docker Compose
cd kafka
docker-compose up -d


Check containers:

docker ps


You should see:

zookeeper – Confluent Zookeeper 7.6.0

kafka – Confluent Kafka Broker 7.6.0

2. Kafka Topic Creation
docker exec -it kafka kafka-topics \
  --create --topic raw.flow --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics \
  --create --topic norm.flow --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics \
  --create --topic alerts.flow --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1


List topics:

docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

3. Dataset Preparation

Download CIC-IDS-2017 from Canadian Institute for Cybersecurity
.

Create a 50,000-row sample:

python data_proc.py


This creates:

data_processed/CICIDS2017_processed.csv

4. Model Training (Week 2)

Train Isolation Forest anomaly detection model:

python src/train_model.py


Expected output:

Training Isolation Forest on 50000 records...
Model saved at model/isolation_forest.joblib

5. Streaming Pipeline (Week 1–2)
Start Producer
python src/producer.py

Start Consumer and Parser
python src/utils/consumer_parser.py

Start Model Inference
python src/utils/feature_infer.py


Expected output:

Isolation Forest model loaded successfully.
Scoring events in real time...
[100] score=-0.241 anomaly=True
[200] score=-0.105 anomaly=False

6. Explainability (Week 3)

Run SHAP Explainability:

python src/explain_shap.py


Expected output:

Loading model and sample data for SHAP analysis...
SHAP analysis complete. Saved to visuals/shap_summary.png


This generates:

visuals/shap_summary.png


showing the top features influencing anomaly detection.

7. Flask Dashboard (Week 3)

Launch the real-time dashboard to visualize alerts.

Start Flask App:

python src/app.py


Dashboard available at:

http://localhost:5000

Displays:

Live streaming alerts (source/destination IP, port, score, anomaly status)

Auto-refresh every 3 seconds

Integrated SHAP summary visualization (optional)

Example UI:

+------------+---------------+---------------+--------+--------+-----------+
| Timestamp  | Source IP     | Destination IP| Port   | Score  | Anomaly   |
+------------+---------------+---------------+--------+--------+-----------+
| ...        | 10.0.0.1      | 10.0.0.2      | 443    | -0.234 | True      |

8. Final Architecture Overview
graph TD
A[Producer.py<br/>Mock Flow Logs] -->|raw.flow| B[Consumer_Parser.py<br/>Normalization]
B -->|norm.flow| C[Feature_Infer.py<br/>Isolation Forest Scoring]
C -->|alerts.flow| D[Flask Dashboard<br/>app.py]
C -->|SHAP| E[Explainability<br/>visuals/shap_summary.png]

9. Deliverables Summary
Week	Component	Description	Status
1	Kafka Infrastructure	Dockerized Zookeeper and Kafka	✔️
1	Data Ingestion	Producer → raw.flow	✔️
1	Parser	Consumer → norm.flow	✔️
2	Model Training	Isolation Forest	✔️
2	Real-Time Scoring	norm.flow → alerts.flow	✔️
3	Explainability	SHAP integration	✔️
3	Visualization	Flask web dashboard	✔️
10. How to Run the Complete Application
# 1. Start Kafka
cd kafka && docker-compose up -d
cd ..

# 2. Prepare dataset and train model
python data_proc.py
python src/train_model.py
python src/explain_shap.py

# 3. Run full pipeline (each in a separate terminal)
python src/producer.py
python src/utils/consumer_parser.py
python src/utils/feature_infer.py
python src/app.py


Open in browser:
http://localhost:5000

11. References

CIC IDS 2017 Dataset

Kafka Python Client Documentation

SHAP Library Documentation

Flask Official Documentation