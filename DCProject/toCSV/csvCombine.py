import glob
import os
import pandas as pd

directory_in_str = "..\\output\\fit\\"
directory_out_str = "..\\output\\csv\\"

if not os.path.exists(directory_out_str):
    os.makedirs(directory_out_str)

csv_files = glob.glob(directory_in_str+'\\*_data.{}'.format('csv'))
out = pd.DataFrame()
output_file = directory_out_str + 'output_test2.csv'
first_iteration = True
count = 0
for file in csv_files:
    count += 1
    df = pd.read_csv(file, low_memory=False)
    file_name = file.split('\\')[-1]
    # array = ['Workout_'+str(count)] * df.shape[0]
    array = [file_name] * df.shape[0]
    df['ID'] = array
    if first_iteration:
        out = df
        first_iteration = False
    else:
        out = pd.concat([out, df], axis=0, join='outer')
out=out[['record.timestamp[s]', 'record.distance[m]', 'record.heart_rate[bpm]', 'record.power[watts]', 'record.enhanced_speed[m/s]', 'ID']]
out.rename(columns={'record.timestamp[s]': 'timestamp[s]', 'record.distance[m]': 'distance[m]', 'record.heart_rate[bpm]': 'heart_rate[bpm]', 'record.power[watts]': 'power[watts]', 'record.enhanced_speed[m/s]': 'speed[m/s]'}, errors="raise", inplace=True)
out.fillna(0, inplace=True)

out.to_csv(output_file, index=False)
print("Complete")