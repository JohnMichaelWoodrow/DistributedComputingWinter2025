import gzip
import shutil
import os

from zipfile import ZipFile

# loading the zip file and unzip it to the output folder
with ZipFile("activities.zip", 'r') as zObject:
    zObject.extractall(path="output")

# output directory for files
directory_in_str = "output\\activities\\"
if not os.path.exists(directory_in_str):
    os.makedirs(directory_in_str)

# output directory for unzipped files
directory_out_str = "output\\fit\\"
if not os.path.exists(directory_out_str):
    os.makedirs(directory_out_str)

directory = os.fsencode(directory_in_str)

for file in os.listdir(directory):
    filename = os.fsdecode(file)
    with gzip.open(directory_in_str + filename, 'rb') as f_in:
        with open(directory_out_str + filename.replace(".gz", ""), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
print("unzipped")