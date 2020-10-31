"""Script that inserts file paths to reading text in table publication.
Created by Anna Movall and Jonas Lillqvist in February 2020"""

import mysql.connector
import psycopg2
import json
from pathlib import Path
import re
from fuzzywuzzy import fuzz

from create_comment_data import create_file_list
from create_comment_data import iterate_through_folders
from create_comment_data import read_dict_from_file
from create_comment_data import compare_pubnames_with_filenames

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

# get relevant info from publication table using select with collection id
def get_publication_info(new_collection_id):
    fetch_query = """SELECT id, name, legacy_id FROM publication WHERE publication_collection_id = %s"""
    cursor_new.execute(fetch_query, (new_collection_id,))
    publication_info = cursor_new.fetchall()
    return publication_info

# compare letters' identifiers with all of the collection's file names (containing the same identifiers) to find out each letter's original file path
def compare_letters_with_filenames(publication_id, filepath_list, match_count, publication_count):
    publication_id_dict = read_dict_from_file("id_dictionaries/publication_ids.json")
    fetch_query = """SELECT p_FM from publications WHERE p_id = %s"""
    # use new publication id to find out old id using id dictionary
    # then use old id to fetch letter identifier from old database
    for key, value in publication_id_dict.items():
        if value == publication_id:
            old_publication_id = int(key)
            cursor_old.execute(fetch_query, (old_publication_id,))
            signum = cursor_old.fetchone()[0]
            if signum is None or signum == "":
                original_path = None
                break
            signum = signum.strip()
            found = False
            i = 0
            # for filepath in filepath_list:
            while found == False and i < len(filepath_list):
                original_path = filepath_list[i]
                # get filename without suffix
                filepath = filepath_list[i].stem
                # most letter filepaths contain an identifier in this form:
                search_str = re.compile(r"Br\d{1,4}$")
                # search for an identifier in the filepath
                match_str = re.search(search_str, filepath)
                # if the file path contains no identifier, skip to next file path in the list
                if match_str is None:
                    i += 1
                    continue
                # if the identifier is found, save the matched string in a variable
                match_str = match_str.group(0)
                # compare the matched string to identifier from old database
                if match_str == signum:
                    found = True
                    match_count += 1
                    break # exit loop if the two values match
                i += 1
            if not found:
                original_path = None
    publication_count += 1
    return original_path, match_count, publication_count

# update table publication with original_filename for each publication, if the file name has been found
def update_publication(log_found, publication_name, original_filename, publication_id):
    log_found.write("PUBLICATION: " + publication_name + " MATCHED " + original_filename + "\n")
    update_query = """UPDATE publication SET original_filename = %s WHERE id = %s"""
    values_to_insert = (original_filename, publication_id)
    cursor_new.execute(update_query, values_to_insert)

# reads csv and creates dictionary for update of table publication with original_filename, for collection Publicistik, Forelasningar and Lasning for barn
def create_dict_from_csv(filename):
    with open(filename, encoding="utf-8") as source_file:
        rows = source_file.readlines()
        info_dict = {}
        for row in rows:
            row = row.rstrip()
            elements = row.split(";")
            info_dict[elements[0]] = elements[1]
        return(info_dict)

def main():
    collection_id_dict = read_dict_from_file("id_dictionaries/collection_ids.json")
    old_collections = [(1, "../../Topelius SVN/documents/trunk/Ljungblommor"), (2, "../../Topelius SVN/documents/trunk/Nya_blad_och_Ljung"), (4, "../../Topelius SVN/documents/trunk/Noveller"), (5, "../../Topelius SVN/documents/trunk/Hertiginnan_af_Finland_och_andra_historiska_noveller"), (7, "../../Topelius SVN/documents/trunk/Vinterqvallar"), (12, "../../Topelius SVN/documents/trunk/Finland_framstalldt_i_teckningar"), (16, "../../Topelius SVN/documents/trunk/Ovrig_lyrik"), (18, "../../Topelius SVN/documents/trunk/Noveller_och_kortprosa"), (24, "../../Topelius SVN/documents/trunk/Academica"), (30, "../../Topelius SVN/documents/trunk/Brev/Forlagskorrespondens"), (6, "../../Topelius SVN/documents/trunk/Faltskarns_berattelser"), (8, "../../Topelius SVN/documents/trunk/Planeternas_skyddslingar"), (10, "../../Topelius SVN/documents/trunk/Naturens_bok_och_Boken_om_vart_land"), (13, "../../Topelius SVN/documents/trunk/En_resa_i_Finland"),  (17, "../../Topelius SVN/documents/trunk/Dramatik"), (19, "../../Topelius SVN/documents/trunk/Ovrig_barnlitteratur"), (20, "../../Topelius SVN/documents/trunk/Forelasningar"), (22, "../../Topelius SVN/documents/trunk/Finland_i_19de_seklet"), (23, "../../Topelius SVN/documents/trunk/Publicistik"), (26, "../../Topelius SVN/documents/trunk/Religiosa_skrifter_och_psalmer"), (29, "../../Topelius SVN/documents/trunk/Dagbocker"), (31, "../../Topelius SVN/documents/trunk/Brev/Foraldrakorrespondens"), (32, "../../Topelius SVN/documents/trunk/Lasning_for_barn")]
    # initialize counters for match log statistics
    publication_count = 0
    match_count = 0
    # create log files
    log_found = open("logs/matched_reading_texts.txt", "w", encoding="utf-8")
    log_not_found = open("logs/unmatched_reading_texts.txt", "w", encoding="utf-8")
    # loop through collections and publications in them
    for collection in old_collections:
        old_id = collection[0]
        collection_path = collection[1]
        new_collection_id = collection_id_dict[str(old_id)] # get new collection id using dictionary
        publication_info = get_publication_info(new_collection_id) # select publications with this collection id from table publication
        filepath_list = create_file_list(collection_path)
        for tuple in publication_info:
            publication_name = tuple[1]
            publication_id = tuple[0]
            legacy_id = tuple[2]
            if old_id < 30 and old_id != 23 and old_id != 20: # don't use this comparison function for Brev, Publicistik, Forelasningar
                filepath, match_count, publication_count = compare_pubnames_with_filenames(publication_name, filepath_list, match_count, publication_count)
                # if the publication has a matching file path, update table publication and write match to log file
                if filepath is not None:
                    original_filename = filepath.as_posix().replace("../../Topelius SVN/", "") # create file path string and shorten it
                    update_publication(log_found, publication_name, original_filename, publication_id)
                # if no matching file path was found, write this to log file
                else:
                    log_not_found.write("Publication name: " + publication_name + "\n")
            elif old_id == 30 or old_id == 31: # Brev have their own comparison function; otherwise the same as above
                filepath, match_count, publication_count = compare_letters_with_filenames(publication_id, filepath_list, match_count, publication_count)
                if filepath is not None:
                    original_filename = filepath.as_posix().replace("../../Topelius SVN/", "") # create file path string and shorten it
                    update_publication(log_found, publication_name, original_filename, publication_id)
                else:
                    log_not_found.write("Publication name: " + publication_name + "\n")   
            elif old_id == 23: # matching file paths for Publicistik are kept in a separate document
                publicistik_info_dict = create_dict_from_csv("csv/ZTS_Publicistik_verk_signum_filer.csv")
                # get file name from dictionary using legacy_id
                if legacy_id in publicistik_info_dict.keys():
                    filename = publicistik_info_dict[legacy_id]
                    year = filename[0:4] # get year from file name and use it as folder name
                    original_filename = "documents/trunk/Publicistik/" + year + "/" + filename
                    update_publication(log_found, publication_name, original_filename, publication_id)
                else:
                    log_not_found.write("Publication name: " + publication_name + "\n")
            elif old_id == 20: # matching file paths for Forelasningar are kept in a separate document; they are all there; otherwise as above
                forelasningar_info_dict = create_dict_from_csv("csv/Forelasningar_signum_filer.csv")
                filename = forelasningar_info_dict[legacy_id]
                original_filename = "documents/trunk/Forelasningar/" + filename
                update_publication(log_found, publication_name, original_filename, publication_id)
            elif old_id == 32: # matching file paths for Lasning for barn are kept in a separate document
                lfb_info_dict = create_dict_from_csv("csv/Lfb_signum_filer.csv")
                original_filename = lfb_info_dict[legacy_id]
                update_publication(log_found, publication_name, original_filename, publication_id)         
    conn_new_db.commit()
    log_found.write("\nPublications matched: " + str(match_count) + "/" + str(publication_count) + ". Percentage matched: " + str(match_count/publication_count*100))
    log_found.close()
    log_not_found.close()
    conn_new_db.close()
    cursor_new.close()
    conn_old_db.close()
    cursor_old.close()
    
main()