"""
Script that updates table publication_version with file paths for the version's original xml file.
Created by Anna Movall and Jonas Lillqvist in February/March 2020.
"""

import psycopg2
import json
from pathlib import Path
import re
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz

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

# get relevant info from table publication_version using select with collection id
def get_version_info(new_collection_id):
    fetch_query = """SELECT publication_version.id, publication.name, publication_version.legacy_id, publication_version.name FROM publication_version, publication WHERE publication_collection_id = %s AND publication.id = publication_id"""
    cursor_new.execute(fetch_query, (new_collection_id,))
    version_info = cursor_new.fetchall()
    return version_info

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
        elif content.suffix == ".xml":
            filelist.append(content)

# opens an xml file from the web sever (the script uses a local copy of the files)
# extracts its body element and removes added attributes to make it as similar as possible to the original xml file
# returns the body for comparison
def get_body_from_web_xml(filepath):
    with open(filepath, encoding="utf-8") as source_file:
        soup = BeautifulSoup(source_file, "xml")
        body = soup.body
        for tag in body.find_all("l"):
            del tag["n"]
        for tag in body.find_all("lg"):
            del tag["xml:id"]
        for tag in body.find_all("p"):
            del tag["xml:id"]
        return str(body)

# opens an original xml file from SVN and extracts its body element
# returns the body for comparison
def get_body_from_xml(filepath):
    with filepath.open(encoding="utf-8", errors='ignore') as source_file:
        soup = BeautifulSoup(source_file, "xml")
        body = soup.body
        return str(body)

# opens an original xml file from SVN and extracts the text content of its title element
def get_title_from_xml(filepath):
    with filepath.open(encoding="utf-8", errors='ignore') as source_file:
        soup = BeautifulSoup(source_file, "xml")
        title = soup.title.get_text()
        return title

# updates table publication_version with original_filename
def update_publication_version(version_id, original_filename):
    update_query = """UPDATE publication_version SET original_filename = %s WHERE id = %s"""
    values_to_insert = (original_filename, version_id)
    cursor_new.execute(update_query, values_to_insert)

# receives the publication name connected to the version and compares it to the folder names in the list of all file paths for this collection
# matching paths are added to a list
# we use partial match because folder names are sometimes shortened or altered versions of the publication name
def compare_pub_name_with_directories(pub_name, filepath_list):
    # remove special characters from publication names
    search_str = re.sub(r",|\.|\?|!|–|’|»|:|(|)|\[|\]|&", "", pub_name).strip()
    search_str = search_str.replace(" ", "_").lower()
    search_str = search_str.replace("-", "_")
    search_str = search_str.replace("ä", "a")
    search_str = search_str.replace("å", "a")
    search_str = search_str.replace("ö", "o")
    search_str = search_str.replace("é", "e")
    search_str = search_str.replace("ü", "u")
    search_str = search_str.replace("æ", "ae")
    i = 0
    match_list = []
    while i < len(filepath_list):
        original_path = filepath_list[i]
        dir_name = original_path.parts[-2].lower() # gets the last directory in the file path, this folder contains the versions of a specific publication
        match_ratio = fuzz.partial_ratio(search_str, dir_name) # compares publication name and folder name
        if match_ratio == 100:
            match_list.append(original_path) # appends possible original paths for this version to a list
        i += 1
    return match_list

# check for duplicate file paths in log for matched versions; every file path should appear only once
# duplicates are a sign of corrupt data that needs to be corrected manually
def check_for_duplicate_file_paths():
    log_duplicate_matched_versions = open("logs/duplicate_matched_versions.txt", "w", encoding="utf-8")
    with open("logs/matched_versions.txt", "r", encoding="utf-8") as source_file:
        all_text = source_file.read()
        regex = re.compile(r"ORIGINAL PATH: .*?\.xml")
        list_of_original_paths = re.findall(regex, all_text)
        while len(list_of_original_paths) > 0:
            last_item = list_of_original_paths.pop()
            if last_item in list_of_original_paths:
                log_duplicate_matched_versions.write("Duplicate: " + last_item + "\n")
    log_duplicate_matched_versions.close()
       
def main():
    collection_id_dict = read_dict_from_file("id_dictionaries/collection_ids.json")
    # list of collections with collection id and path to folder containing version files
    old_collections = [(32, "../../Topelius SVN/documents/Varianter/Lasning_for_barn_varianter")]
    # initialize counters for match log statistics
    version_count = 0
    match_count = 0
    log_directory_not_found = open("logs/version_directory_not_found_lfb.txt", "w", encoding="utf-8")
    log_directory_found = open("logs/version_directory_found_lfb.txt", "w", encoding="utf-8")
    log_matched_versions = open("logs/matched_versions_lfb.txt", "w", encoding="utf-8")
    log_unmatched_versions = open("logs/unmatched_versions_lfb.txt", "w", encoding="utf-8")
    for collection in old_collections:
        old_id = collection[0]
        collection_path = collection[1]
        new_collection_id = collection_id_dict[str(old_id)] # get new collection id using dictionary
        version_info = get_version_info(new_collection_id)
        filepath_list = create_file_list(collection_path) # create list of all version file paths in this collection
        for tuple in version_info:
            version_count += 1
            version_id = tuple[0]
            pub_name = tuple[1]
            web_xml_filepath = "var/" + tuple[2]
            version_name = tuple[3].strip()
            original_path_list = compare_pub_name_with_directories(pub_name, filepath_list)
            # if no directory and thus no possible file paths were found for this version:
            if len(original_path_list) == 0:
                log_directory_not_found.write("PUBLICATION NAME: " + pub_name + " VERSION ID: " + str(version_id) + "\n")
            else:
                log_directory_found.write("PUBLICATION NAME: " + pub_name + " VERSION ID: " + str(version_id) + "\nPATH LIST: " + "\n")
                for path in original_path_list:
                    log_directory_found.write(path.as_posix() + "\n")
                # for each file path with a folder name matching the publication name, get the content of the title element of the file and compare it with the version name
                filepath_match_list = []
                for path in original_path_list:
                    title = get_title_from_xml(path).strip()
                    # use for most collections:
                    #if title == version_name:
                    #use for lasning for barn:
                    version_name = version_name.replace(",", "")
                    title = title.replace("(", "")
                    title = title.replace(")", "")
                    title = title.replace(",", "")
                    if version_name in title:
                        filepath_match_list.append(path)
                found = False
                # if title matched version_name just once, we have found the file path
                if len(filepath_match_list) == 1:
                    original_filepath = filepath_match_list[0]
                    found = True
                # if title matched version_name more than once, we need to compare the content of the web xml file and the original (SVN) files in order to find the correct file path
                elif len(filepath_match_list) > 1:
                    web_xml_body = get_body_from_web_xml(web_xml_filepath)
                    for path in filepath_match_list:
                        content = get_body_from_xml(path)
                        score = fuzz.ratio(web_xml_body, content)
                        if score >= 90:
                            original_filepath = path
                            found = True
                # if we have found a file path for the version, update table publication_version with original_filename
                if found:
                    match_count += 1
                    original_filename = original_filepath.as_posix().replace("../../Topelius SVN/", "") # shorten file path string
                    update_publication_version(version_id, original_filename)
                    log_matched_versions.write("\nPUBLICATION NAME: " + pub_name + " WEB XML PATH: " + web_xml_filepath + "\nORIGINAL PATH: " + original_filename)
                else:
                    log_unmatched_versions.write("\nPUBLICATION NAME: " + pub_name + " WEB XML PATH: " + web_xml_filepath)
    conn_new_db.commit()
    log_matched_versions.write("\nVersions matched: " + str(match_count) + "/" + str(version_count) + ". Percentage matched: " + str(match_count/version_count*100))
    log_directory_found.close()
    log_directory_not_found.close()
    log_matched_versions.close()
    log_unmatched_versions.close()
    check_for_duplicate_file_paths()
    conn_new_db.close()
    cursor_new.close()

main()