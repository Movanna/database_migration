"""Script that inserts data into table publication_comment and updates table publication with the corresponding publication_comment_id. 
It finds out the original filename for the comment file and inserts it into publication_comment (this info did not exist in the old database).
Created by Anna Movall and Jonas Lillqvist in February 2020"""

import psycopg2
import json
from pathlib import Path
import re
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

# get relevant info from publication table using select with collection id
def get_info_from_publication(new_collection_id):
    publication_info = []
    fetch_query = """SELECT id, name, published, legacy_id FROM publication WHERE publication_collection_id = %s"""
    cursor_new.execute(fetch_query, (new_collection_id,))
    publication_info = cursor_new.fetchall()
    return publication_info

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

# compare publication name with the collection's file names to find file path to general comment (or reading text, when called upon from insert_filepaths.py)
def compare_pubnames_with_filenames(publication_name, filepath_list, match_count, publication_count):
    # remove special characters from publication names
    search_str = re.sub(r",|\.|\?|!|–|’|»|:|(|)|\[|\]|&", "", publication_name).strip()
    search_str = search_str.replace(" ", "_").lower()
    search_str = search_str.replace("-", "_")
    search_str = search_str.replace("ä", "a")
    search_str = search_str.replace("å", "a")
    search_str = search_str.replace("ö", "o")
    search_str = search_str.replace("é", "e")
    search_str = search_str.replace("ü", "u")
    search_str = search_str.replace("æ", "ae")
    found = False
    i = 0
    # for filepath in filepath_list:
    while found == False and i < len(filepath_list):
        original_path = filepath_list[i]
        # get filename without suffix
        filepath = filepath_list[i].stem
        # remove special characters and useless stuff from filename
        filepath = filepath.replace("K ", "")
        filepath = filepath.replace(" tg", "")
        filepath = filepath.replace(" ", "_").lower()
        filepath = filepath.replace("-", "_")
        filepath = filepath.replace("ä", "a")
        filepath = filepath.replace("å", "a")
        filepath = filepath.replace("ö", "o")
        filepath = filepath.replace("é", "e")
        filepath = filepath.replace("æ", "ae")
        filepath = filepath.replace("æ", "ae")
        filepath = re.sub(r"_komm$|\[|\]", "", filepath)
        filepath = filepath.replace("_Academica", "")
        filepath = filepath.replace("brev_komm_", "") # for letters
        # compare publication name with file name:
        if fuzz.partial_ratio(search_str, filepath) == 100:
            found = True
            match_count += 1
            break
        i += 1
    if not found:
        original_path = None
    publication_count += 1
    return original_path, match_count, publication_count


def main():
    collection_id_dict = read_dict_from_file("id_dictionaries/collection_ids.json")
    # list of all collections with collection id and path to folder with general comments; collections without general comments use a template file:
    old_collections = [(1, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Ljungblommor"), (2, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Nya_blad_och_Ljung"), (4, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Noveller"), (5, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Hertiginnan_af_Finland_och_andra_historiska_noveller"), (7, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Vinterqvallar"), (12, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Finland_framstalldt_i_teckningar"), (16, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Ovrig_lyrik"), (18, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Noveller_och_kortprosa"), (24, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Academica"), (30, "../../Topelius SVN/documents/Redaktionella_texter/Kommentarer/Brev/Forlagskorrespondens"), (6, "templates/comment.xml"), (8, "templates/comment.xml"), (10, "templates/comment.xml"), (13, "templates/comment.xml"), (20, "templates/comment.xml"), (22, "templates/comment.xml"), (23, "templates/comment.xml"), (29, "templates/comment.xml"), (31, "templates/comment.xml")]
    template_path = "templates/comment.xml"
    # initialize counters for match log statistics
    publication_count = 0
    match_count = 0
    # create log files
    log_found = open("logs/matched_comments.txt", "w", encoding="utf-8")
    log_not_found = open("logs/unmatched_comments.txt", "w", encoding="utf-8")
    # loop through collections and publications in them
    for collection in old_collections:
        old_id = collection[0]
        collection_path = collection[1]
        new_collection_id = collection_id_dict[str(old_id)] # get new collection id using dictionary
        publication_info = get_info_from_publication(new_collection_id) # select publications with this collection id from table publication
        # get all file paths from collection's folder, if there is one
        if collection_path != template_path:
            filepath_list = create_file_list(collection_path)
        # get info about one publication, match name with file path if needed, create a row in publication_comment and update publication with comment id
        for tuple in publication_info:
            publication_name = tuple[1]
            # check if collection has a general comment; if yes, get the comment's filepath through the comparison function 
            if collection_path != template_path:
                comment_filepath, match_count, publication_count = compare_pubnames_with_filenames(publication_name, filepath_list, match_count, publication_count)
                # if the publication has a matching file path, write match to log file and store file path in shortened form in a variable
                if comment_filepath is not None:
                    log_found.write("PUBLICATION: " + publication_name + " MATCHED " + comment_filepath.as_posix() + "\n")
                    original_filename = comment_filepath.as_posix().replace("../../Topelius SVN/", "") # create filepath string and shorten it   
                # use Null value if there is no matching file path           
                else:
                    original_filename = None
                    log_not_found.write("Publication name: " + publication_name + "\n")
            # if there is no general comment, use template path for original filename
            else:
                original_filename = template_path
            published = tuple[2]
            legacy_id = tuple[3]
            # insert file path or template path and some info about the publication into table publication_comment
            insert_query = """INSERT INTO publication_comment(published, legacy_id, original_filename) VALUES (%s, %s, %s) RETURNING id"""
            values_to_insert = (published, legacy_id, original_filename)
            cursor_new.execute(insert_query, values_to_insert)
            # get newly created comment id
            comment_id = cursor_new.fetchone()[0]
            publication_id = tuple[0]
            # update table publication with the comment id for this publication
            update_query = """UPDATE publication SET publication_comment_id = %s WHERE id = %s"""
            values_to_insert = (comment_id, publication_id)
            cursor_new.execute(update_query, values_to_insert)
    conn_new_db.commit()
    log_found.write("\nPublications matched: " + str(match_count) + "/" + str(publication_count) + ". Percentage matched: " + str(match_count/publication_count*100))
    log_found.close()
    log_not_found.close()
    conn_new_db.close()
    cursor_new.close()

main()