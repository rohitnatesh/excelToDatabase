import os
import pandas as pd
from mysql.connector import connect, Error

current_directory = os.getcwd()
data_directory_path = os.path.join(current_directory, "data")

# Get file names in 'data' folder.
data_file_names = os.listdir(data_directory_path)
data_file_names = [
    f for f in data_file_names if os.path.isfile(f"{data_directory_path}/{f}")
]

for file_name in data_file_names:
    data = pd.read_excel(f"{data_directory_path}/{file_name}")

    [schema, raw_table] = file_name.split("#")
    table = raw_table.split(".")[0]
    columns = data.columns.values.tolist()

    try:
        with connect(
            host="localhost",
            user="root",
            password="root",
        ) as connection:
            with connection.cursor() as cursor:
                for index, row in data.iterrows():
                    query = f"SELECT * FROM {schema}.{table} WHERE id={row['id']}"
                    result = cursor.execute(query)

                    if cursor.fetchone():
                        update_query = f"UPDATE {schema}.{table} SET "

                        for column in columns:
                            update_query += f'{column}="{row[column]}", '
                        update_query = update_query[:-2]
                        update_query += f" WHERE id={row['id']};"

                        cursor.execute(update_query)
                        connection.commit()
                    else:
                        insert_query = f"INSERT INTO {schema}.{table} VALUES ("

                        for column in columns:
                            insert_query += f'"{row[column]}",'
                        insert_query = insert_query[:-1]
                        insert_query += ");"

                        cursor.execute(insert_query)
                        connection.commit()
    except Error as e:
        print(e)
