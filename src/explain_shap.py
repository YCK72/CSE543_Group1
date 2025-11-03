import joblib
import shap
import pandas as pd
from utils.feature_engineer import batch_features

def main():
    print("Loading model and sample data for SHAP analysis...")
    model = joblib.load('model/isolation_forest.joblib')
    df = batch_features('data_processed/CICIDS2017_processed.csv').sample(500)
    explainer = shap.Explainer(model, df)
    shap_values = explainer(df)

    print("SHAP analysis complete. Saving summary plot...")
    shap.summary_plot(shap_values, df, show=False)
    import matplotlib.pyplot as plt
    plt.tight_layout()
    plt.savefig('visuals/shap_summary.png')
    print("Saved SHAP summary to visuals/shap_summary.png")

if __name__ == "__main__":
    main()
