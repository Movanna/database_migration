"""Script that inserts data for introductions and title pages into db and updates table publication_collection with the corresponding id:s.
Created by Anna Movall and Jonas Lillqvist in April 2020"""

import psycopg2
import re
import json

conn_new_db = psycopg2.connect(
    host="",
    database="",
    user="",
    port="",
    password=""
)
cursor_new = conn_new_db.cursor()

PROJECT_ID = 10 # enter the current project id
INTRODUCTION_FILE_PATH = "documents/Redaktionella_texter/Inledningar/"
TITLE_PAGE_FILE_PATH = "documents/Redaktionella_texter/Titelsidor/"

# get relevant info from table publication_collection
def get_info_from_publication_collection(PROJECT_ID):
    collection_info = []
    fetch_query = """SELECT id, published FROM publication_collection WHERE project_id = %s"""
    cursor_new.execute(fetch_query, (PROJECT_ID,))
    collection_info = cursor_new.fetchall()
    return collection_info

# the name bases for the xml files (title and introduction) are stored in a csv together with old collection ids
def create_list_from_csv(filename):
    with open(filename, "r", encoding="utf-8") as source_file:
        collection_name_list = []
        for line in source_file:
            row = line.rstrip()
            elements = row.split(";")
            collection_name_list.append(elements)
    return collection_name_list

def read_dict_from_file(filename):
    with open(filename, encoding="utf-8") as source_file:
        json_content = json.load(source_file)
        return json_content

# create a dictionary with new collection id as key and file name base as value
def create_collection_name_dict(collection_names_with_old_id, collection_id_dict):
    coll_name_dict = {}
    for row in collection_names_with_old_id:
        old_coll_id = row[0]
        coll_name = row[1]
        new_coll_id = collection_id_dict[old_coll_id]
        coll_name_dict[new_coll_id] = coll_name
    return coll_name_dict

# insert data into table publication_collection_introduction
def create_publication_collection_introduction(published, introduction_original_filename):
    insert_query = """INSERT INTO publication_collection_introduction(published, original_filename) VALUES (%s, %s) RETURNING id"""
    values_to_insert = (published, introduction_original_filename)
    cursor_new.execute(insert_query, values_to_insert)
    introduction_id = cursor_new.fetchone()[0]
    return introduction_id

# insert data into table publication_collection_title
def create_publication_collection_title(published, title_page_original_filename):
    insert_query = """INSERT INTO publication_collection_title(published, original_filename) VALUES (%s, %s) RETURNING id"""
    values_to_insert = (published, title_page_original_filename)
    cursor_new.execute(insert_query, values_to_insert)
    title_page_id = cursor_new.fetchone()[0]
    return title_page_id

# update table publication_collection with the ids for introduction and title page
def update_publication_collection(introduction_id, title_page_id, collection_id):
    update_query = """UPDATE publication_collection SET publication_collection_introduction_id=%s, publication_collection_title_id=%s WHERE id=%s"""
    values_to_insert = (introduction_id, title_page_id, collection_id)
    cursor_new.execute(update_query, values_to_insert)

def main():
    collection_info = get_info_from_publication_collection(PROJECT_ID)
    # create list of old collection ids and collection names for file name bases
    collection_names_with_old_id = create_list_from_csv("csv/introduction_title_names.csv")
    # create dict mapping old and new collection ids
    collection_id_dict = read_dict_from_file("id_dictionaries/collection_ids.json")
    # create dict mapping new ids and file name bases
    collection_name_dict_with_new_ids = create_collection_name_dict(collection_names_with_old_id, collection_id_dict)
    for collection in collection_info:
        collection_id = collection[0]
        published = collection[1]
        name = collection_name_dict_with_new_ids[collection_id]
        introduction_original_filename = INTRODUCTION_FILE_PATH + name + "_inl.xml"
        introduction_id = create_publication_collection_introduction(published, introduction_original_filename)
        title_page_original_filename = TITLE_PAGE_FILE_PATH + name + "_tit.xml"
        title_page_id = create_publication_collection_title(published, title_page_original_filename)
        update_publication_collection(introduction_id, title_page_id, collection_id)
    conn_new_db.commit()
    cursor_new.close()
    conn_new_db.close()
       
main()