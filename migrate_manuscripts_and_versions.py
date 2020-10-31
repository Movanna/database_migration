"""Script that migrates data to tables publication_manuscript and publication_version.
Created by Anna Movall and Jonas Lillqvist in January 2020"""

import mysql.connector
import psycopg2
import json

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

def read_dict_from_file(filename):
    with open(filename, encoding="utf-8") as source_file:
        json_content = json.load(source_file)
        return json_content

def create_publication_manuscript(publication_id_dict):
    fetch_query = """SELECT m_id, m_publication_id, m_title, m_sort, m_filename, m_type, m_section_id FROM manuscripts"""
    cursor_old.execute(fetch_query)
    old_tuples = cursor_old.fetchall()
    insert_query = """INSERT INTO publication_manuscript(publication_id, name, sort_order, legacy_id, type, section_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"""
    manuscript_id_dictionary = {}
    for tuple in old_tuples:
        # if a manuscript does not belong to a published publication, it should not be migrated
        old_publication_id = tuple[1]
        if str(old_publication_id) not in publication_id_dict.keys():
            continue
        old_id = tuple[0]
        publication_id = publication_id_dict[str(old_publication_id)] # get new id from dictionary using old id as key; this value is a string in the json dictionary file
        title = tuple[2]
        sort_order = tuple[3]
        legacy_id = tuple[4]
        type = tuple[5]
        section_id = tuple[6]
        if section_id is not None:
            section_id = int(section_id.replace("ch", "")) # remove ch, the id is an int in the new db
        values_to_insert = (publication_id, title, sort_order, legacy_id, type, section_id)
        cursor_new.execute(insert_query, values_to_insert)
        new_id = cursor_new.fetchone()[0]
        manuscript_id_dictionary[old_id] = new_id
    conn_new_db.commit()
    return manuscript_id_dictionary

def create_publication_version(publication_id_dict):
    fetch_query = """SELECT v_id, v_publication_id, v_title, v_sort, v_type, v_filename, v_section_id FROM versions"""
    cursor_old.execute(fetch_query)
    old_tuples = cursor_old.fetchall()
    insert_query = """INSERT INTO publication_version (publication_id, name, sort_order, type, legacy_id, section_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"""
    version_id_dict = {}
    for tuple in old_tuples:
        old_publication_id = tuple[1]
        # if a version does not belong to a published publication, it should not be migrated
        if str(old_publication_id) not in publication_id_dict.keys():
            continue
        old_id = tuple[0]
        publication_id = publication_id_dict[str(old_publication_id)] # get new id from dictionary using old id as key; this value is a string in the json dictionary file
        title = tuple[2]
        sort_order = tuple[3]
        type = tuple[4]
        legacy_id = tuple[5]
        section_id = tuple[6]
        if section_id is not None:
            section_id = int(section_id.replace("ch", "")) # remove ch, the id is an int in the new db
        values_to_insert = (publication_id, title, sort_order, type, legacy_id, section_id)
        cursor_new.execute(insert_query, values_to_insert)
        new_id = cursor_new.fetchone()[0]
        version_id_dict[old_id] = new_id
    conn_new_db.commit()
    return version_id_dict

def write_dict_to_file(dictionary, filename):
    json_dict = json.dumps(dictionary)
    with open(filename, "w", encoding="utf-8") as output_file:
        output_file.write(json_dict)
        print("Dictionary written to file.")

def main():
    publication_id_dict = read_dict_from_file("id_dictionaries/publication_ids.json")
    manuscript_id_dict = create_publication_manuscript(publication_id_dict)
    write_dict_to_file(manuscript_id_dict, "id_dictionaries/manuscript_ids.json")
    version_id_dict = create_publication_version(publication_id_dict)
    write_dict_to_file(version_id_dict, "id_dictionaries/version_ids.json")
    conn_new_db.close()
    cursor_new.close()
    conn_old_db.close()
    cursor_old.close()
    
main()