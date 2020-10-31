"""Script that migrates data to tables publication_collection and publication.
Created by Anna Movall and Jonas Lillqvist in January 2020"""

import mysql.connector
import psycopg2
import json

PROJECT_NAME = ""

conn_old_db = mysql.connector.connect(
    host="",
    database="",
    user="",
    passwd=""
)
cursor_old = conn_old_db.cursor()

conn_new_db = psycopg2.connect(
    host="",
    database="",
    user="",
    port="",
    password=""
)
cursor_new = conn_new_db.cursor()

def create_project(PROJECT_NAME):
    insert_query = """INSERT INTO project (published, name) VALUES (%s, %s) RETURNING id"""
    insert_row = (1, PROJECT_NAME)
    cursor_new.execute(insert_query, insert_row)
    id = cursor_new.fetchone()[0]
    conn_new_db.commit()
    return id

def create_collection(project_id):
    fetch_query = """SELECT zts_id, zts_title, zts_lansering FROM publications_zts"""
    cursor_old.execute(fetch_query)
    old_tuples = cursor_old.fetchall()
    insert_query = """INSERT INTO publication_collection(project_id, published, name, legacy_id) VALUES (%s, %s, %s, %s) RETURNING id"""
    id_dictionary = {}
    for tuple in old_tuples:
        old_id = tuple[0]
        title = tuple[1]
        lansering = tuple[2]
        values_to_insert = (project_id, lansering, title, old_id)
        # don't include Brev, which has been split into two new collections instead (zts_id 30 and 31) and don't include the old collection of Lfb
        if old_id != 15 and old_id != 9:
            cursor_new.execute(insert_query, values_to_insert)
            new_id = cursor_new.fetchone()[0]
            id_dictionary[old_id] = new_id
    conn_new_db.commit()
    return id_dictionary

def create_publication(collection_id_dictionary):
    fetch_query = """SELECT p_id, p_title, p_zts_id, p_coll_id, p_identifier, p_maskindatum, p_genre FROM publications"""
    cursor_old.execute(fetch_query)
    old_tuples = cursor_old.fetchall()
    insert_query = """INSERT INTO publication(name, publication_collection_id, legacy_id, original_publication_date, genre) VALUES (%s, %s, %s, %s, %s) RETURNING id"""
     # old ids saved as keys with new ids as values; needed for connecting manuscripts, versions and facsimiles to publications.
    id_dictionary = {}
    for tuple in old_tuples:
        old_id = tuple[0]
        title = tuple[1]
        old_collection_id = tuple[2]
        old_subcollection_id = tuple[3]
        if old_collection_id == 15 and old_subcollection_id == 1: # text belongs to Förlagskorrespondens
            collection_id = collection_id_dictionary[30] # old collection Brev was split and two new collections were added, but not to table publications; the p_zts_id for Förlagskorrespondens is 30
        elif old_collection_id == 15 and old_subcollection_id == 2: # text belongs to Föräldrakorrespondens
            collection_id = collection_id_dictionary[31] # old collection Brev was split and two new collections were added, but not to table publications; the p_zts_id for Föräldrakorrespondens is 31
        elif old_collection_id == 15:
            continue # skip other texts belonging to Brev; they haven't been published
        elif old_collection_id not in collection_id_dictionary.keys():
            continue # do not migrate unpublished texts
        else:
            collection_id = collection_id_dictionary[tuple[2]] # for everything except Brev, get new id from dictionary using old id as key
        old_text_id = tuple[4] # p_identifier -> legacy_id
        original_date = tuple[5]
        # replace mysql-dates in format 0000-00-00 or empty strings with NULL:
        if original_date == "0000-00-00" or original_date == "":
            original_date = None
        if original_date is not None:
            original_date = original_date.replace("-00", "-XX").strip() # replace mysql-dates in format -00-00 with -XX-XX
        genre = tuple[6]
        values_to_insert = (title, collection_id, old_text_id, original_date, genre)
        cursor_new.execute(insert_query, values_to_insert)
        new_id = cursor_new.fetchone()[0]
        id_dictionary[old_id] = new_id
    conn_new_db.commit() 
    return id_dictionary

def write_dict_to_file(dictionary, filename):
    json_dict = json.dumps(dictionary)
    with open(filename, "w", encoding="utf-8") as output_file:
        output_file.write(json_dict)
        print("Dictionary written to file", filename)

# set value of published in table publication according to the corresponding value from table publication_collection;
# the field does not exist in the old table publications, so it can't be transferred directly
def set_published_in_publication(project_id):
    fetch_query = """SELECT id, published FROM publication_collection WHERE project_id=%s"""
    cursor_new.execute(fetch_query, (project_id,))
    tuples = cursor_new.fetchall()
    for tuple in tuples:
        collection_id = tuple[0]
        published = tuple[1]
        update_query = """UPDATE publication SET published=%s WHERE publication_collection_id=%s"""
        cursor_new.execute(update_query, (published, collection_id))
    conn_new_db.commit()

def main():
    project_id = create_project(PROJECT_NAME)
    collection_ids = create_collection(project_id)
    write_dict_to_file(collection_ids, "id_dictionaries/collection_ids.json")
    publication_ids = create_publication(collection_ids)
    write_dict_to_file(publication_ids, "id_dictionaries/publication_ids.json")
    set_published_in_publication(project_id)
    conn_new_db.close()
    cursor_new.close()
    conn_old_db.close()
    cursor_old.close()

main()