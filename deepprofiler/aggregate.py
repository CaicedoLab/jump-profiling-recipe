import pandas as pd
import os
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool

def log_error_plates(path):
    with open("skipped_plates.txt", "a") as log:
        log.write(f"{path}\n")

'''
Function used to get the well level aggregated statistics
'''

def well_level_parquet(data_path, output_dir):
    plate_name = data_path.split("/")[-1]
    files = os.listdir(data_path)
    all_data = []
    errors = False
    for file in files:
        try:
            data = pd.read_parquet(os.path.join(data_path, file, "embedding.parquet"))
            all_data.append(data)
        except:
            print("Error with",os.path.join(data_path, file, "embedding.parquet"))
            errors = True
            break
    if not errors:
        all_data = pd.concat(all_data)
        all_data = all_data.groupby(["source", "batch", "plate", "well"])["all_emb"].mean().reset_index()
        all_data.to_parquet(os.path.join(output_dir, f"{plate_name}.parquet"), index=False)
    else:
        print("Skipping plate",plate_name)
        log_error_plates(data_path)

'''
Process a single plate
'''
def process_plate(params):
    input_dir, batch, plate = params
    if not os.path.exists(input_dir + f"/profiles/Cell_Painting_CNN_conv6a_b3667957/{batch}/{plate}"):
        os.makedirs(input_dir + f"/profiles/Cell_Painting_CNN_conv6a_b3667957/{batch}/{plate}")
    well_dir_input = input_dir + f"/embeddings/Cell_Painting_CNN_conv6a_b3667957/{batch}/{plate}"
    well_dir_output = input_dir + f"/profiles/Cell_Painting_CNN_conv6a_b3667957/{batch}/{plate}"
    well_level_parquet(data_path=well_dir_input, output_dir=well_dir_output)
    print(f"Plate {plate} done")

'''
Check if there is a directory called profiles, 
if there is not create one and copy everything present in the embeddings
directory to the profiles directory
'''
def store_source_parquet(input_dir):
    if not os.path.exists(input_dir + "/profiles"):
        os.makedirs(input_dir + "/profiles")
    batches = os.listdir(input_dir + "/embeddings/Cell_Painting_CNN_conv6a_b3667957")
    if not os.path.exists(input_dir + "/profiles/Cell_Painting_CNN_conv6a_b3667957"):
        os.makedirs(input_dir + "/profiles/Cell_Painting_CNN_conv6a_b3667957")
    for batch in batches:
        plates = os.listdir(input_dir + f"/embeddings/Cell_Painting_CNN_conv6a_b3667957/{batch}")
        if not os.path.exists(input_dir + f"/profiles/Cell_Painting_CNN_conv6a_b3667957/{batch}"):
            os.makedirs(input_dir + f"/profiles/Cell_Painting_CNN_conv6a_b3667957/{batch}")
        with Pool() as p:
            p.map(process_plate, [(input_dir, batch, plate) for plate in plates])

