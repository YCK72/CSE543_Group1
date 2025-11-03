import pandas as pd
import numpy as np
import re

TARGET_COLS = [
    "Flow Bytes/s", "Flow Packets/s", "Flow Duration",
    "Total Fwd Packets", "Total Backward Packets",
    "Total Length of Fwd Packets", "Total Length of Bwd Packets",
    "Fwd Packet Length Mean", "Bwd Packet Length Mean",
    "Destination Port"
]

def normalize_text(t: str) -> str:
    t = t.replace("\ufeff", "")
    t = re.sub(r"[^A-Za-z0-9/ ]", " ", t)
    t = re.sub(r"\s+", " ", t.strip())
    return t.lower()

def find_best_matches(cols, target_cols):
    norm_map = {normalize_text(c): c for c in cols}
    matches = {}
    for t in target_cols:
        nt = normalize_text(t)
        best = next((norm_map[c] for c in norm_map if nt in c or c in nt), None)
        if best:
            matches[t] = best
    return matches

def batch_features(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    match = find_best_matches(df.columns, TARGET_COLS)

    print(f"✅ Matched columns ({len(match)}/{len(TARGET_COLS)}):")
    for k, v in match.items():
        print(f"  {k} ← {v}")

    if len(match) == 0:
        raise ValueError("No matching numeric columns found. Check dataset headers.")

    # keep only matched numeric columns
    df = df[list(match.values())].fillna(0)
    df.columns = list(match.keys())
    df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
    return df

def build_feature_vector(record: dict) -> np.ndarray:
    vec = []
    for col in TARGET_COLS:
        val = record.get(col, 0)
        try:
            vec.append(float(val))
        except (ValueError, TypeError):
            vec.append(0.0)
    return np.array(vec, dtype=float)
