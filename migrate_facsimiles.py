"""Script that migrates data to tables publication_facsimile_collection and publication_facsimile.
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

def write_dict_to_file(dictionary, filename):
    json_dict = json.dumps(dictionary)
    with open(filename, "w", encoding="utf-8") as output_file:
        output_file.write(json_dict)
        print("Dictionary written to file", filename)

def write_text_to_file(text, filename):
    with open(filename, "w", encoding="utf-8") as output_file:
        output_file.write(text)
        print("Text written to file", filename)

def create_facsimile_collection():
    fetch_query = """SELECT publication_id, title, description, pages, pre_page_count, pages_comment, facs_url FROM facsimiles"""
    cursor_old.execute(fetch_query)
    old_tuples = cursor_old.fetchall()
    insert_query = """INSERT INTO publication_facsimile_collection(title, description, number_of_pages, start_page_number, page_comment, external_url) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"""
    facsimile_coll_id_dict = {}
    for tuple in old_tuples:
        old_id = tuple[0]
        values_to_insert = (tuple[1], tuple[2], tuple[3], tuple[4], tuple[5], tuple[6])
        cursor_new.execute(insert_query, values_to_insert)
        new_id = cursor_new.fetchone()[0]
        facsimile_coll_id_dict[old_id] = new_id
    conn_new_db.commit()
    return facsimile_coll_id_dict

# this table connects publications (= texts) and facsimiles
def create_publication_facsimile(publication_id_dict, facsimile_coll_id_dict, manuscript_id_dict):
    fetch_query = """SELECT publications_id, section_id, facs_id, page_nr, priority, type, ms_id FROM facsimile_publications"""
    cursor_old.execute(fetch_query)
    old_tuples = cursor_old.fetchall()
    insert_query = """INSERT INTO publication_facsimile(publication_id, section_id, publication_facsimile_collection_id, page_nr, priority, type, publication_manuscript_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    excluded_tuples = "" # for saving info about tuples in the old db that are excluded from migration
    for tuple in old_tuples:
        old_publication_id = tuple[0]
        # do not include tuples which refer to unpublished texts:
        if str(old_publication_id) not in publication_id_dict.keys():
            excluded_tuples += "tuple with old_publication_id: " + str(old_publication_id) + " skipped \n"
            continue
        publication_id = publication_id_dict[str(old_publication_id)] #get new id from dictionary using old id as key; this value is a string in the json dictionary file
        section_id = tuple[1]
        # the new db requires an int value for section_id and does not accept null
        if section_id is None:
            section_id = 0
        if isinstance(section_id, str): # check that the old value is a string before making replace, to avoid error
            section_id = int(section_id.replace("ch", "")) # section_id is of type int in new db
        old_facsimile_id = tuple[2]
        if str(old_facsimile_id) not in facsimile_coll_id_dict.keys(): # skip tuples which refer to unpublished facsimiles
            excluded_tuples += "tuple with old_facsimile_id: " + str(old_facsimile_id) + " skipped\n"
            continue
        facsimile_id = facsimile_coll_id_dict[str(tuple[2])]
        page_nr = tuple[3]
        priority = tuple[4]
        type = tuple[5]
        old_manuscript_id = tuple[6]
        # only NULL, 0 or values in the dictionary are allowed for ms_id; otherwise the tuple should be skipped because it refers to an unpublished manuscript
        if old_manuscript_id is not None and old_manuscript_id != 0 and str(old_manuscript_id) not in manuscript_id_dict.keys():
            excluded_tuples += "tuple with old_ms_id: " + str(old_manuscript_id) + " skipped\n"
            continue
        if str(old_manuscript_id) in manuscript_id_dict.keys():
            manuscript_id = manuscript_id_dict[str(old_manuscript_id)] # get new id from dictionary using old id as key
        elif old_manuscript_id == 0:
            manuscript_id = None # use NULL if old value is 0
        else:
            manuscript_id = None # otherwise the old value is NULL, which is preserved
        values_to_insert = (publication_id, section_id, facsimile_id, page_nr, priority, type, manuscript_id)
        cursor_new.execute(insert_query, values_to_insert)
    conn_new_db.commit()
    write_text_to_file(excluded_tuples, "logs/excluded_facsimile_publications_tuples.txt")

def main():
    facsimile_coll_id_dict = create_facsimile_collection()
    write_dict_to_file(facsimile_coll_id_dict, "id_dictionaries/facsimile_coll_ids.json")
    publication_id_dict = read_dict_from_file("id_dictionaries/publication_ids.json")
    facsimile_coll_id_dict = read_dict_from_file("id_dictionaries/facsimile_coll_ids.json")
    manuscript_id_dict = read_dict_from_file("id_dictionaries/manuscript_ids.json")
    create_publication_facsimile(publication_id_dict, facsimile_coll_id_dict, manuscript_id_dict)
    cursor_new.close()
    conn_new_db.close()
    conn_old_db.close()
    cursor_old.close()

main()