import os
import sys
import aggregate
import subprocess

if len(sys.argv) < 3:
    print("Use: run.py src_dir work_dir")
    print("     src_dir: Path to single cell features")
    print("     dest_dir: Path to store the output")
    sys.exit()

'''
Read script parameters
'''
srcdir = sys.argv[1]
workdir = sys.argv[2]

all_zips = [x for x in os.listdir(srcdir) if x.endswith(".zip")]


'''
Wait until a number of zip files are transferred and uncompressed
'''
def wait(workdir, max_items):
    remaining = len([x for x in os.listdir(workdir) if x.endswith(".zip")])
    while remaining > max_items:
        print("Waiting for",remaining,"files to be uncompressed")
        os.system("sleep 10")
        remaining = len([x for x in os.listdir(workdir) if x.endswith(".zip")])


'''
Function to move the data from long-term storage to a local working directory
'''
def prepare_data(src, max_parallel_files=20):
    src_zips = [x for x in all_zips if x.startswith(src+"_")]

    for z in src_zips:
        os.system(f"cp {srcdir}/{z} {workdir} && cd {workdir} && unzip {z} && rm {z} &")
        wait(workdir, max_parallel_files)

    wait(workdir, 0)
    print("Data ready for processing")

'''
Function to fix the typo in the directory names
'''
def fix_paths(root):
    for dir in os.listdir(root):
        print(dir)
        offending = [x for x in os.listdir(os.path.join(root, dir, "workspace_dl", "embeddings")) if x.find("Patining") != -1]
        for o in offending:
            path_from = os.path.join(root, dir, "workspace_dl/embeddings", o)
            path_to = path_from.replace("Cell_Patining_CNN", "Cell_Painting_CNN")
            instruction = f"mv {path_from} {path_to}"
            print(instruction)
            os.system(instruction)


'''
Main loop
'''
sources = set([x.split("_")[0] for x in os.listdir(srcdir) if x.startswith("source")])
sources = list(sources)
sources.sort()
print(sources)

for src in ['source10', 'source2', 'source3', 'source4', 'source5', 'source6', 'source7', 'source8', 'source9']:
    prepare_data(src)
    fix_paths(f"{workdir}/cpg0016-jump/")
    sn = int(src.replace("source",""))
    aggregate.store_source_parquet(f"{workdir}/cpg0016-jump/source_{sn}/workspace_dl/")

    ## TODO:
    ## zip each plate separately and move it back to the storage system
    os.system(f"rm -Rf {workdir}/cpg0016-jump/source_{sn}/")



