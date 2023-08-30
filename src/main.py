import os
import pandas as pd

# Get file names in 'data' folder.

current_directory = os.getcwd()
data_directory_path = os.path.join(current_directory, "data")

data_file_names = os.listdir(data_directory_path)
data_file_names = [
    f
    for f in data_file_names
    if os.path.isfile(data_directory_path + "/" + f)
]

for file_name in data_file_names:
    data = pd.read_excel(data_directory_path + "/" + file_name)
    
    [schema, table] =  file_name.split('#')
    columns = data.columns.values.tolist()

    for row in data.iterrows():
        print(row)
