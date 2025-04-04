import os
import subprocess

# Specify fit files directory
directory_in_str = "..\\output\\fit\\"
directory_in = os.fsencode(directory_in_str)

# run the batch file on each file that ends with .fit
for file in os.listdir(directory_in):
        filename = os.fsdecode(file)
        if filename.endswith(".fit"):
            p = subprocess.Popen(['GarminSDK\\java\\FitToCSV-record.bat', "..\\output\\fit\\" + filename], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            p.stdin.write('\n')
            p.stdin.flush()
            p.wait()
print("conversion done")