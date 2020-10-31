"""
Script used for updating table document in db topelius_notes with filepaths to comments for Lasning for barn.
Created by Anna Movall and Jonas Lillqvist in May 2020.
"""

import mysql.connector

conn_old_db = mysql.connector.connect(
    host="",
    database="",
    user="",
    passwd=""
)
cursor_old = conn_old_db.cursor()

# creates a list from csv file with legacy id and file path
def create_list_from_csv(filename):
    with open(filename, "r", encoding="utf-8") as source_file:
        lfb_list = []
        for line in source_file:
            row = line.rstrip()
            elements = row.split(";")
            lfb_list.append(elements)
        return lfb_list

def insert_document(filepath, title):
    insert_query = """INSERT INTO document(path, title) VALUES(%s, %s)"""
    values = (filepath, title)
    cursor_old.execute(insert_query, values)

def main():
    filepath_list = create_list_from_csv("csv/Lfb_signum_filer.csv")
    for row in filepath_list:
        filepath = "/" + row[1]
        title = "abc"
        insert_document(filepath, title)
    conn_old_db.commit()
    cursor_old.close()
    conn_old_db.close()

main()