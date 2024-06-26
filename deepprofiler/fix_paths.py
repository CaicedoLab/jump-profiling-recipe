import os
import sys

if len(sys.argv) < 2:
    print("Use: fix_paths.py root")
    sys.exit()

root = sys.argv[1]

for dir in os.listdir(root):
    print(dir)
    offending = [x for x in os.listdir(os.path.join(root, dir, "workspace_dl", "embeddings")) if x.find("Patining") != -1]
    for o in offending:
        path_from = os.path.join(root, dir, "workspace_dl/embeddings", o)
        path_to = path_from.replace("Cell_Patining_CNN", "Cell_Painting_CNN")
        instruction = f"mv {path_from} {path_to}"
        print(instruction)
        os.system(instruction)
