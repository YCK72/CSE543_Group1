import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from utils.feature_engineer import batch_features

def main():
    df = batch_features('data_processed/CICIDS2017_processed.csv')
    print(f"Training Isolation Forest on {len(df)} records...")
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42
    )
    model.fit(df)
    joblib.dump(model, 'model/isolation_forest.joblib')
    print("Model saved at model/isolation_forest.joblib")

if __name__ == "__main__":
    main()
