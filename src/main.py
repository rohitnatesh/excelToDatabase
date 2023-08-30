import os
import pandas as pd
from mysql.connector import connect, Error

# Get the current directory and compute the absolute path to the 'data' folder.
current_directory = os.getcwd()
data_directory_path = os.path.join(current_directory, "data")

# Get file names in 'data' folder.
data_file_names = os.listdir(data_directory_path)
# Filtering to get only the files.
# TODO: Check the file extensions also.
data_file_names = [
    f for f in data_file_names if os.path.isfile(f"{data_directory_path}/{f}")
]

# For each file.
for file_name in data_file_names:
    # Read the excel.
    data = pd.read_excel(f"{data_directory_path}/{file_name}")

    # Files are named in the format SCHEMA#TABLE_NAME.
    # Get the table details from the file name.
    [schema, raw_table] = file_name.split("#")
    # Remove file extension.
    table = raw_table.split(".")[0]
    # Get all the column names in the excel, which corresponds to SQL columns names as well.
    columns = data.columns.values.tolist()

    try:
        # Make DB connection.
        # TODO: The file name can be unique keys to entries in another file having DB details.
        with connect(
            host="localhost",
            user="root",
            password="root",
        ) as connection:
            with connection.cursor() as cursor:
                # For every row in the excel.
                for index, row in data.iterrows():
                    # Try to get the row in the SQL.
                    query = f"SELECT * FROM {schema}.{table} WHERE id={row['id']}"
                    result = cursor.execute(query)

                    # If row exists in SQL, then the existing row should be updated with the new details.
                    if cursor.fetchone():
                        update_query = f"UPDATE {schema}.{table} SET "

                        for column in columns:
                            update_query += f'{column}="{row[column]}", '
                        update_query = update_query[:-2]
                        update_query += f" WHERE id={row['id']};"

                        cursor.execute(update_query)
                        connection.commit()
                    # Else, a new row entry should be created.
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
