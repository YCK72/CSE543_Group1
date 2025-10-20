import pandas as pd
df = pd.read_csv('data_original/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv')
df_sample = df.sample(n=50000, random_state=42)
df_sample.to_csv('data_processed/CICIDS2017_processed.csv', index=False)
