import pandas as pd
import os

df = pd.read_csv("data/letterboxd_members.csv")

for i in range(3):
    dfs = df[i*1000:i*1000+1000]
    dfs.to_csv(f"data/members_{i+1}.csv", mode='a', index=False, header=not os.path.exists(f"data/members_{i+1}.csv"))