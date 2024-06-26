import pandas as pd
import os
import numpy as np


def well_level_parquet(data_path, output_dir):
    plate_name = data_path.split("/")[-1]
    print(plate_name)
    files = os.listdir(data_path)
    all_data = pd.DataFrame()
    for file in files:
        data = pd.read_parquet(data_path + f"/{file}/embedding.parquet")
        mean_data = {"source": np.unique(data["source"]), "batch": np.unique(data["batch"]), 
                                      "plate": np.unique(data["plate"]), "well": np.unique(data["well"]), "all_emb": [data["all_emb"].mean()]}
        parquet_mean = pd.DataFrame.from_dict(mean_data)
        all_data = pd.concat([all_data, parquet_mean])
    all_data.to_parquet(output_dir + f"/{plate_name}.parquet", index=False)