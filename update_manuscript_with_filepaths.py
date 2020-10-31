"""Script that inserts file paths to manuscripts in table publication_manuscript.
Created by Anna Movall and Jonas Lillqvist in February 2020"""

import psycopg2
import json
from pathlib import Path
import re
from bs4 import BeautifulSoup

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

# get relevant info from table publication_manuscript using select with collection id
def get_manuscript_info(new_collection_id):
    fetch_query = """SELECT publication_manuscript.id, publication_manuscript.name FROM publication_manuscript, publication WHERE publication_collection_id = %s AND publication.id = publication_id"""
    cursor_new.execute(fetch_query, (new_collection_id,))
    manuscript_info = cursor_new.fetchall()
    return manuscript_info

# create path object for folder from given filepath string, save all paths to files found in this folder or subfolders in a list
def create_file_list(filepath):
    path = Path(filepath)
    filelist = []
    iterate_through_folders(path, filelist)
    return filelist

# iterate through folders recursively and append filepaths to list
def iterate_through_folders(path, filelist):
    for content in path.iterdir():
        if content.is_dir():
            iterate_through_folders(content, filelist)
        else:
            filelist.append(content)

# loop through list of all file paths to manuscript xml files, open the files and get content of title element
# create dictionary with title as key and file path as value
# the content of the title element was used to create the name of the manuscript in the old database, therefore it can be used to match manuscripts and file paths
def create_title_path_dict(filepath_list):
    manuscript_title_path_dict = {}
    duplicate_titles = []
    for path in filepath_list:
        with path.open(encoding="utf-8") as source_file:
            soup = BeautifulSoup(source_file, 'xml')
            tei_header = soup.find('teiHeader')
            title = tei_header.find('title').text.strip()
            # add title to dictionary, if it isn't there
            if title not in manuscript_title_path_dict.keys():
                manuscript_title_path_dict[title] = path
            # if the title is already in the dictionary (it is used in several files)
            # add title to list of duplicates
            else:
                duplicate_titles.append((title, path.as_posix()))
    # remove titles that are not unique (used in several files) from dictionary
    # add title + filepath to list to make the list complete; these need to be checked manually
    for item in duplicate_titles:
        title = item[0]
        if title in manuscript_title_path_dict.keys():
            dict_filepath = manuscript_title_path_dict.pop(title)
            duplicate_titles.append((title, dict_filepath.as_posix()))
    return manuscript_title_path_dict, duplicate_titles

# update table publication_manuscript with original_filename
def update_publication_manuscript(manuscript_id, original_filename):
    update_query = """UPDATE publication_manuscript SET original_filename = %s WHERE id = %s"""
    values_to_insert = (original_filename, manuscript_id)
    cursor_new.execute(update_query, values_to_insert)

def main():
    collection_id_dict = read_dict_from_file("id_dictionaries/collection_ids.json")
    # list of collections with collection id and path to folder containing manuscript files
    old_collections = [(1, "../../Topelius SVN/documents/Manuskript/Ljungblommor_manuskript"), (2, "../../Topelius SVN/documents/Manuskript/Nya_blad_och_Ljung_manuskript"), (16, "../../Topelius SVN/documents/trunk/Ovrig_lyrik"), (24, "../../Topelius SVN/documents/trunk/Academica/Otryckta Academica texter"), (30, "../../Topelius SVN/documents/trunk/Brev/Forlagskorrespondens"), (17, "../../Topelius SVN/documents/trunk/Dramatik"), (19, "../../Topelius SVN/documents/Manuskript/Ovrig_barnlitteratur_manuskript"), (20, "../../Topelius SVN/documents/trunk/Forelasningar"), (29, "../../Topelius SVN/documents/trunk/Dagbocker"), (31, "../../Topelius SVN/documents/trunk/Brev/Foraldrakorrespondens"), (32, "../../Topelius SVN/documents/Manuskript/Lasning_for_barn_manuskript")]
    # initialize counters for match log statistics
    manuscript_count = 0
    match_count = 0
    log_found = open("logs/matched_manuscripts.txt", "w", encoding="utf-8")
    log_not_found = open("logs/unmatched_manuscripts.txt", "w", encoding="utf-8")
    log_not_found.write("The following manuscripts have no files connected to them.\n")
    log_files_with_same_title = open("logs/manuscript_files_with_same_title.txt", "w", encoding="utf-8")
    for collection in old_collections:
        old_id = collection[0]
        collection_path = collection[1]
        new_collection_id = collection_id_dict[str(old_id)] # get new collection id using dictionary
        manuscript_info = get_manuscript_info(new_collection_id) # select manuscripts with this collection id from table publication
        filepath_list = create_file_list(collection_path) # create list of all manuscript file paths in this collection
        # create dictionary with title from xml file as key and file path as value
        manuscript_title_path_dict, duplicate_titles = create_title_path_dict(filepath_list)
        for item in duplicate_titles:
            log_files_with_same_title.write("TITLE: " + item[0] + " PATH: " + item[1] + "\n")
        for tuple in manuscript_info:
            manuscript_count += 1
            manuscript_id = tuple[0]
            manuscript_name = tuple[1].strip()
            # manuscript_name in database was originally created from the title element in the xml file for the manuscript
            # if it matches a title (key) in the dictionary, we know it's filepath (value)
            if manuscript_name in manuscript_title_path_dict.keys():
                filepath = manuscript_title_path_dict[manuscript_name]
                original_filename = filepath.as_posix().replace("../../Topelius SVN/", "") # create file path string and shorten it
                log_found.write("MANUSCRIPT NAME: " + manuscript_name + " MATCHED " + original_filename + "\n")
                match_count += 1
                # add original_filepath for manuscript in database
                update_publication_manuscript(manuscript_id, original_filename)
            else:
                log_not_found.write("MANUSCRIPT NAME: " + manuscript_name + " MANUSCRIPT ID: " + str(manuscript_id) + "\n")
    conn_new_db.commit()
    log_found.write("\nManuscripts matched: " + str(match_count) + "/" + str(manuscript_count) + ". Percentage matched: " + str(match_count/manuscript_count*100))
    log_found.close()
    log_not_found.close()
    log_files_with_same_title.close()
    conn_new_db.close()
    cursor_new.close()
        
main()